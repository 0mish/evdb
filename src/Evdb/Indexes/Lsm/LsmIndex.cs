using Evdb.IO;

namespace Evdb.Indexes.Lsm;

public sealed class LsmIndex : IIndex
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

    public string Path { get; }

    public LsmIndex(LsmIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));
        ArgumentNullException.ThrowIfNull(options.Path, nameof(options.Path));
        ArgumentNullException.ThrowIfNull(options.FileSystem, nameof(options.FileSystem));

        Path = options.Path;

        // Create the locks.
        _sync = new object();

        // Create the index directory.
        _fs = options.FileSystem;
        _fs.CreateDirectory(Path);

        // Create compaction infrastructure.
        _compactionQueue = new CompactionQueue();
        _compactionThread = new CompactionThread(_compactionQueue);

        // Create or load manifest file.
        //
        // TODO:
        //
        // Re-consider the API design. We are performing IO in the constructor, which may not be expected? Perhaps
        // people would like to control when IO occurs.
        _manifest = new Manifest(_fs);

        // Create the in memory tables.
        _l0 = new VirtualTable(_fs, new FileMetadata(FileType.Log, _manifest.NextFileNumber()), options.VirtualTableSize);
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
                _compactionQueue.Enqueue(new CompactionJob(_l0, OnCompacted));

                _l0n.Add(_l0);
                _l0 = new VirtualTable(_fs, new FileMetadata(FileType.Log, _manifest.NextFileNumber()), _l0.MaxSize);
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
        ManifestRevision revision = _manifest.Current;

        lock (_sync)
        {
            l0n = _l0n.ToArray();
        }

        // FIXME: l0n is not sorted by newest to oldest.
        foreach (VirtualTable table in l0n)
        {
            if (table.TryGet(ikey, out value))
            {
                return true;
            }
        }

        foreach (FileMetadata file in revision.Files)
        {
            if (file.TryGetTable(out PhysicalTable? table) && table.TryGet(ikey, out value))
            {
                return true;
            }
        }

        return false;
    }

    private void OnCompacted(VirtualTable vtable, PhysicalTable ptable)
    {
        ManifestEdit edit = new()
        {
            FilesRegistered = new List<FileId>() { ptable.Metadata.Id }
        };

        _manifest.Commit(edit);

        lock (_sync)
        {
            _l0n.Remove(vtable);
        }
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
