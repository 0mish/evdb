using Evdb.IO;

namespace Evdb.Indexing.Lsm;

internal sealed class LsmIndex : IIndex
{
    private int _disposed;

    // Locks.
    private readonly object _sync;

    // Tables.
    private VirtualTable _l0;
    private readonly List<VirtualTable> _l0n;

    // Compactions.
    private readonly CompactionQueue _compactionQueue;
    private readonly CompactionThread _compactionThread;

    // Logs.
    private readonly Manifest _manifest;

    // Other.
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

        _compactionQueue = new CompactionQueue();
        _compactionThread = new CompactionThread(_compactionQueue);

        _l0 = new VirtualTable(_fs, new FileMetadata(_manifest.Path, FileType.Log, _manifest.NextFileNumber()), options.VirtualTableSize);
        _l0n = new List<VirtualTable>();
    }

    public bool TrySet(string key, in ReadOnlySpan<byte> value)
    {
        if (Volatile.Read(ref _disposed) == 1)
        {
            return false;
        }

        IndexKey ikey = new(key, _manifest.VersionNumber);

        lock (_sync)
        {
            while (!_l0.TrySet(ikey, value))
            {
                _compactionQueue.Enqueue(new CompactionJob(_l0, CompactTable));

                _l0n.Add(_l0);
                _l0 = new VirtualTable(_fs, new FileMetadata(_manifest.Path, FileType.Log, _manifest.NextFileNumber()), _l0.MaxSize);
            }
        }

        return true;
    }

    public bool TryGet(string key, out ReadOnlySpan<byte> value)
    {
        if (Volatile.Read(ref _disposed) == 1)
        {
            value = default;

            return false;
        }

        IndexKey ikey = new(key, _manifest.VersionNumber);
        VirtualTable l0 = Volatile.Read(ref _l0);

        if (l0.TryGet(ikey, out value))
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

            // FIXME: l0n is not sorted by newest to oldest.
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
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        // CompactionQueue.Dispose() returns control to the caller when the queue is empty.
        _compactionQueue.Dispose();
        _compactionThread.Dispose();

        _l0.Dispose();

        lock (_sync)
        {
            foreach (VirtualTable table in _l0n)
            {
                table.Dispose();
            }
        }

        _manifest.Dispose();
    }
}
