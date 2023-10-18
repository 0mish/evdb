using Evdb.Indexing.Format;
using Evdb.IO;

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
    private readonly IFileSystem _fs;
    private readonly IBlockCache _blockCache;

    public bool IsCompacting => _compactionQueue.Count > 0;

    public Database(DatabaseOptions options)
    {
        _options = options;
        _fs = options.FileSystem;

        _sync = new object();

        // TODO:
        //
        // Re-consider the API design. We are performing IO in the constructor, which may not be expected? Perhaps
        // people would like to control when IO occurs.
        _manifest = new Manifest(_fs, options.Path);

        // TODO:
        //
        // Make the number of compaction thread configurable.
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
            EpochGC.Acquire();

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
            EpochGC.Release();
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
            EpochGC.Acquire();

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
            EpochGC.Release();
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
            EpochGC.Acquire();

            using Iterator iter = new(_manifest.Current);

            for (iter.MoveTo(startKey); iter.IsValid && iter.Key.SequenceCompareTo(endKey) <= 0; iter.MoveNext())
            {
                action(iter.Key, iter.Value);
            }
        }
        finally
        {
            EpochGC.Release();
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
        PhysicalLog log = new(_fs, metadata);
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
            EpochGC.Acquire();

            FileMetadata metadata = new(_manifest.Path, FileType.Table, _manifest.NextFileNumber());

            vtable.Flush(_fs, metadata);

            PhysicalTable ptable = new(_fs, metadata, _blockCache);
            ManifestEdit edit = new()
            {
                Registered = new object[] { ptable },
                Unregistered = new object[] { vtable }
            };

            _manifest.Commit(edit);

            // Dispose the table once all thread passes this epoch.
            EpochGC.Defer(vtable.Dispose);
        }
        finally
        {
            EpochGC.Release();
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
