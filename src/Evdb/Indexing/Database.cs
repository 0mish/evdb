using Evdb.Indexing.Format;

namespace Evdb.Indexing;

internal delegate void KeyValueAction(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

internal sealed class Database : IDisposable
{
    private bool _disposed;
    private VirtualTable _table;

    private readonly object _sync;

    private readonly CompactionQueue _compactionQueue;
    private readonly CompactionThread _compactionThread;

    private readonly Manifest _manifest;
    private readonly DatabaseOptions _options;
    private readonly IBlockCache _blockCache;

    public bool IsCompacting => _compactionQueue.Count > 0;

    public Database(DatabaseOptions options)
    {
        _options = options;

        _sync = new object();

        // TODO: Reconsider the API design. We are performing IO in the constructor, which may not be expected? Perhaps
        // people would like to control when IO occurs.
        _manifest = new Manifest(options.FileSystem, options.Path);

        // TODO: Make the number of compaction thread configurable.
        _compactionQueue = new CompactionQueue();
        _compactionThread = new CompactionThread(_compactionQueue);

        _blockCache = new WeakReferenceBlockCache();

        _table = NewTable();
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            return false;
        }

        try
        {
            Epoch.Acquire();

            lock (_sync)
            {
                ulong version = _manifest.VersionNumber;
                ReadOnlySpan<byte> ikey = IndexKey.Encode(key, version);

                while (!_table.TrySet(ikey, value))
                {
                    VirtualTable oldTable = _table;

                    _table = NewTable();
                    _compactionQueue.Enqueue(new CompactionJob(oldTable, CompactTable));
                }

                // FIXME: Advance VersionNumber after key-value inserted.
            }

            return true;
        }
        finally
        {
            Epoch.Release();
        }
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            value = default;

            return false;
        }

        try
        {
            Epoch.Acquire();

            ManifestState state = _manifest.Current;
            ulong version = _manifest.VersionNumber;
            ReadOnlySpan<byte> ikey = IndexKey.Encode(key, version);

            foreach (VirtualTable table in state.VirtualTables)
            {
                if (table.TryGet(ikey, out value))
                {
                    return true;
                }
            }

            foreach (PhysicalTable table in state.PhysicalTables)
            {
                if (table.TryGet(ikey, out value))
                {
                    return true;
                }
            }

            value = default;

            return false;
        }
        finally
        {
            Epoch.Release();
        }
    }

    public bool TryGetRange(ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey, KeyValueAction action)
    {
        if (_disposed)
        {
            return false;
        }

        try
        {
            Epoch.Acquire();

            using Iterator iter = new(_manifest.Current);

            for (iter.MoveTo(startKey); iter.IsValid && iter.Key.SequenceCompareTo(endKey) <= 0; iter.MoveNext())
            {
                action(iter.Key, iter.Value);
            }
        }
        finally
        {
            Epoch.Release();
        }

        return true;
    }

    public Iterator GetIterator()
    {
        // NOTE: Iterators maybe broken because they do not use EBR directly.
        return new Iterator(_manifest.Current);
    }

    private VirtualTable NewTable()
    {
        FileMetadata metadata = new(_manifest.Path, FileType.Log, _manifest.NextFileNumber());
        PhysicalLog log = new(_options.FileSystem, metadata);
        VirtualTable table = new(log, _options.VirtualTableSize);

        ManifestEdit edit = new()
        {
            Registered = new object[] { table, log }
        };

        _manifest.Commit(edit);

        return table;
    }

    private void CompactTable(VirtualTable vtable)
    {
        try
        {
            Epoch.Acquire();

            FileMetadata metadata = new(_manifest.Path, FileType.Table, _manifest.NextFileNumber());

            vtable.Flush(_options.FileSystem, metadata, _options.DataBlockSize, _options.BloomBlockSize);

            PhysicalTable ptable = new(_options.FileSystem, metadata, _blockCache);
            ManifestEdit edit = new()
            {
                Registered = new object[] { ptable },
                Unregistered = new object[] { vtable }
            };

            _manifest.Commit(edit);

            // Dispose the table once all thread passes this epoch.
            //
            // TODO(Optimize):
            // Reconsider vanila EBR for table compaction since a compaction can take a while, we might be holding a
            // lot of retired resources for a while as well.
            Epoch.Defer(vtable.Dispose);
        }
        finally
        {
            Epoch.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // CompactionQueue.Dispose() returns control to the caller when the queue is empty.
        _compactionQueue.Dispose();
        _compactionThread.Dispose();

        _manifest.Dispose();
        _disposed = true;
    }

    public sealed class Iterator : IIterator
    {
        private bool _disposed;
        private readonly MergeIterator _iter;

        public ReadOnlySpan<byte> Key => _iter.Key;
        public ReadOnlySpan<byte> Value => _iter.Value;
        public bool IsValid => _iter.IsValid;

        internal Iterator(ManifestState state)
        {
            List<IIterator> iters = new();

            foreach (VirtualTable table in state.VirtualTables)
            {
                iters.Add(table.GetIterator());
            }

            foreach (PhysicalTable table in state.PhysicalTables)
            {
                iters.Add(table.GetIterator());
            }

            // FIXME: This iterator should be wrapped in an iterator which selects the latest version of the key.
            _iter = new MergeIterator(iters.ToArray());
        }

        public void MoveToFirst()
        {
            _iter.MoveToFirst();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _iter.MoveTo(key);
        }

        public void MoveNext()
        {
            _iter.MoveNext();
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _iter.Dispose();

            _disposed = true;
        }
    }
}
