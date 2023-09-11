using Evdb.IO;

namespace Evdb.Indexing.Lsm;

internal sealed class LsmIndex : IDisposable
{
    private bool _disposed;

    private readonly object _sync;

    private VirtualTable _l0;
    private readonly List<VirtualTable> _l0n;

    private readonly CompactionQueue _compactionQueue;
    private readonly CompactionThread _compactionThread;

    private readonly Manifest _manifest;

    private readonly IFileSystem _fs;

    public LsmIndex(LsmIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));
        ArgumentNullException.ThrowIfNull(options.Path, nameof(options.Path));
        ArgumentNullException.ThrowIfNull(options.FileSystem, nameof(options.FileSystem));

        _sync = new object();

        _fs = options.FileSystem;

        // TODO:
        //
        // Re-consider the API design. We are performing IO in the constructor, which may not be expected? Perhaps
        // people would like to control when IO occurs.
        _manifest = new Manifest(_fs, options.Path, _sync);

        // TODO:
        //
        // Make the number of compaction thread configurable.
        _compactionQueue = new CompactionQueue();
        _compactionThread = new CompactionThread(_compactionQueue);

        _l0 = new VirtualTable(_fs, new FileMetadata(_manifest.Path, FileType.Log, _manifest.NextFileNumber()), options.VirtualTableSize);
        _l0n = new List<VirtualTable>();
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            return false;
        }

        ulong version = _manifest.VersionNumber;
        ReadOnlySpan<byte> ikey = IndexKey.Encode(key, version);

        lock (_sync)
        {
            while (!_l0.TrySet(ikey, value))
            {
                VirtualTable oldL0 = _l0;

                _l0n.Add(_l0);
                _l0 = new VirtualTable(_fs, new FileMetadata(_manifest.Path, FileType.Log, _manifest.NextFileNumber()), _l0.Capacity);

                _compactionQueue.Enqueue(new CompactionJob(oldL0, CompactTable));
            }
        }

        return true;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            value = default;

            return false;
        }

        ulong version = _manifest.VersionNumber;
        ReadOnlySpan<byte> ikey = IndexKey.Encode(key, version);

        if (_l0.TryGet(ikey, out value))
        {
            return true;
        }

        // Make a copy of the _l0n to avoid holding locks for long.
        VirtualTable[] l0n;
        ManifestState state;

        lock (_sync)
        {
            l0n = _l0n.ToArray();
            state = _manifest.Current;
            state.Reference();
        }

        foreach (VirtualTable table in l0n)
        {
            if (table.TryGet(ikey, out value))
            {
                return true;
            }
        }

        foreach (FileId fileId in state.Files)
        {
            if (_manifest.Resolve(fileId) is PhysicalTable table && table.TryGet(ikey, out value))
            {
                return true;
            }
        }

        lock (_sync)
        {
            state.Unreference();
        }

        return false;
    }

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    private void CompactTable(VirtualTable vtable)
    {
        // Flush the virtual table to disk.
        FileMetadata metadata = vtable.Flush(_manifest.Path);

        // Commit the newly flushed PhysicalTable to the manifest so readers can see it.
        ManifestEdit edit = new()
        {
            FilesRegistered = new[] { metadata.Id }
        };

        lock (_sync)
        {
            _manifest.Commit(edit);
            _l0n.Remove(vtable);
        }

        vtable.Dispose();
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

        lock (_sync)
        {
            _l0.Dispose();

            foreach (VirtualTable table in _l0n)
            {
                table.Dispose();
            }
        }

        _manifest.Dispose();
        _disposed = true;
    }

    public class Iterator : IIterator
    {
        private readonly MergeIterator _iter;

        public ReadOnlySpan<byte> Key => _iter.Key;
        public ReadOnlySpan<byte> Value => _iter.Value;

        internal Iterator(LsmIndex index)
        {
            lock (index._sync)
            {
                Manifest manifest = index._manifest;
                ManifestState state = manifest.Current;
                List<IIterator> iters = new();

                foreach (VirtualTable table in index._l0n)
                {
                    iters.Add(table.GetIterator());
                }

                foreach (FileId fileId in state.Files)
                {
                    if (manifest.Resolve(fileId) is PhysicalTable table)
                    {
                        iters.Add(table.GetIterator());
                    }
                }

                iters.Add(index._l0.GetIterator());

                _iter = new MergeIterator(iters.ToArray());
            }
        }

        public bool Valid()
        {
            return _iter.Valid();
        }

        public void MoveToMin()
        {
            _iter.MoveToMin();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _iter.MoveTo(key);
        }

        public void MoveNext()
        {
            _iter.MoveNext();
        }
    }
}
