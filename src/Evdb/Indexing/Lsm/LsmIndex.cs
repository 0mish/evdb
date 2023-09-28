﻿using Evdb.IO;

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

        _l0n = new List<VirtualTable>();
        _l0 = new VirtualTable(NewLog(), options.VirtualTableSize);
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            return false;
        }

        lock (_sync)
        {
            ulong version = _manifest.VersionNumber;
            ReadOnlySpan<byte> ikey = IndexKey.Encode(key, version);

            while (!_l0.TrySet(ikey, value))
            {
                VirtualTable oldL0 = _l0;

                _l0n.Add(_l0);
                _l0 = new VirtualTable(NewLog(), oldL0.Capacity);

                _compactionQueue.Enqueue(new CompactionJob(oldL0, CompactTable));
            }

            // FIXME: Advance VersionNumber after key-value inserted.
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
        ManifestState? state = null;

        try
        {
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
        }
        finally
        {
            lock (_sync)
            {
                state?.Unreference();
            }
        }

        return false;
    }

    public Iterator GetIterator()
    {
        // FIXME: Keep track of opened iterators so we do not accidentally close in use resources.
        return new Iterator(this);
    }

    private PhysicalLog NewLog()
    {
        FileMetadata metadata = new(_manifest.Path, FileType.Log, _manifest.NextFileNumber());
        PhysicalLog log = new(_fs, metadata);

        // FIXME: This unblocks writers and allows more than one writer in the write loop.
#if false
        ManifestEdit edit = new()
        {
            FilesRegistered = new[] { metadata.Id }
        };

        _manifest.Commit(edit);
#endif

        return log;
    }

    private void CompactTable(VirtualTable vtable)
    {
        // Flush the virtual table to disk.
        FileMetadata metadata = vtable.Flush(_fs, _manifest.Path);

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

    public sealed class Iterator : IIterator
    {
        private bool _disposed;
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

                // FIXME: This iterator should be wrapped in an iterator which selects the latest version of the key.
                _iter = new MergeIterator(iters.ToArray());
            }
        }

        public bool Valid()
        {
            return _iter.Valid();
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
