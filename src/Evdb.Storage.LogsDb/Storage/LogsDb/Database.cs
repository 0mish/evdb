using Evdb.Storage.LogsDb.Format;

namespace Evdb.Storage.LogsDb;

public delegate void KeyValueAction(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

public sealed class Database : IDisposable
{
    private bool _disposed;
    private VirtualTable? _table;

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
        _blockCache = new WeakReferenceBlockCache();
        _manifest = new Manifest(options.FileSystem, options.Path, _blockCache, options.ManifestLogSize);

        // TODO: Make the number of compaction thread configurable.
        _compactionQueue = new CompactionQueue();
        _compactionThread = new CompactionThread(_compactionQueue);
    }

    public Status Open()
    {
        _manifest.Open();

        _table = NewTable();

        return Status.Success;
    }

    public Status Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            return Status.Disposed;
        }
        else if (_table == null)
        {
            return Status.Closed;
        }

        try
        {
            Epoch.Acquire();

            lock (_sync)
            {
                while (!_table.Set(key, value).IsSuccess)
                {
                    VirtualTable oldTable = _table;

                    _table = NewTable();
                    _compactionQueue.Enqueue(new CompactionJob(oldTable, CompactTable));
                }
            }

            return Status.Success;
        }
        finally
        {
            Epoch.Release();
        }
    }

    public Status Get(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        value = default;

        if (_disposed)
        {
            return Status.Disposed;
        }

        try
        {
            Epoch.Acquire();

            ManifestState state = _manifest.Current;

            foreach (VirtualTable table in state.VirtualTables)
            {
                Status status = table.Get(key, out value);

                if (status.IsFound || !status.IsSuccess)
                {
                    return status;
                }
            }

            foreach (PhysicalTable table in state.PhysicalTables)
            {
                Status status = table.Get(key, out value);

                if (status.IsFound || !status.IsSuccess)
                {
                    return status;
                }
            }

            return Status.NotFound;
        }
        finally
        {
            Epoch.Release();
        }
    }

    public Status GetRange(ReadOnlySpan<byte> startKey, ReadOnlySpan<byte> endKey, KeyValueAction action)
    {
        if (_disposed)
        {
            return Status.Disposed;
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

        return Status.Success;
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

        log.Open();

        ManifestEdit edit = new(
            plogs: new ListEdit<PhysicalLog>(
                registered: new[] { log }
            ),
            vtables: new ListEdit<VirtualTable>(
                registered: new[] { table }
            )
        );

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

            ptable.Open();

            ManifestEdit edit = new(
                ptables: new ListEdit<PhysicalTable>(
                    registered: new[] { ptable }
                ),
                vtables: new ListEdit<VirtualTable>(
                    unregistered: new[] { vtable }
                )
            );

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
