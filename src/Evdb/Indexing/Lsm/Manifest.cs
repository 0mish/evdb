using Evdb.IO;
using Evdb.Threading;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Text;

namespace Evdb.Indexing.Lsm;

internal sealed class Manifest : IDisposable
{
    private bool _disposed;

    private ulong _versionNumber;
    private ulong _fileNumber;

    private readonly object _sync;
    private readonly object _writerSync;
    private readonly Stream _file;
    private readonly BinaryWriter _writer;
    private readonly IFileSystem _fs;

    // TODO: This should be an LRU cache or something like that to dispose opened file handles automatically.
    private readonly ConcurrentDictionary<FileId, File> _cache;

    public string Path { get; }
    public ulong VersionNumber => _versionNumber;
    public ulong FileNumber => _fileNumber;
    public ManifestState Current { get; private set; } = default!;

    public Manifest(IFileSystem fs, string path, object sync)
    {
        _sync = sync;
        _writerSync = new object();

        _fs = fs;
        _fs.CreateDirectory(path);

        Path = path;

        // TODO: Locate latest valid manifest file.
        FileId id = new(FileType.Manifest, number: 0);

        Recover();

        _file = fs.OpenFile(id.GetPath(path), FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);

        _cache = new ConcurrentDictionary<FileId, File>();
    }

    public ulong NextVersionNumber()
    {
        return _versionNumber++;
    }

    public ulong NextFileNumber()
    {
        return _fileNumber++;
    }

    public File? Resolve(FileId fileId)
    {
        if (fileId.Type != FileType.Table)
        {
            return null;
        }

        if (!_cache.TryGetValue(fileId, out File? file))
        {
            file = new PhysicalTable(_fs, new FileMetadata(Path, fileId.Type, fileId.Number));

            _cache.TryAdd(fileId, file);
        }

        return file;
    }

    public void Commit(ManifestEdit edit)
    {
        edit.VersionNumber = VersionNumber;
        edit.FileNumber = FileNumber;

        List<FileId> files = new(Current.Files);

        foreach (FileId fileId in edit.FilesUnregistered ?? Array.Empty<FileId>())
        {
            files.Remove(fileId);
        }

        foreach (FileId fileId in edit.FilesRegistered ?? Array.Empty<FileId>())
        {
            files.Add(fileId);
        }

        // Append the new state to the front of the state list.
        ManifestState @new = new(edit.VersionNumber.Value, edit.FileNumber.Value, files.ToImmutableArray());
        Current.Next = @new;
        @new.Previous = Current;
        Current = @new;

        bool lockReleased = false;

        try
        {
            MonitorHelper.Exit(_sync, out lockReleased);

            // Use a different lock to serialize writes to the manifest log so the main lock is freed and other
            // operations are allowed meanwhile.
            lock (_writerSync)
            {
                LogEdit(edit);
            }
        }
        finally
        {
            if (lockReleased)
            {
                Monitor.Enter(_sync);
            }
        }
    }

    public void Clean()
    {
        List<FileId> dead = new();
        List<FileId> alive = Current.Files.ToList();
        ManifestState? state = Current.Previous;

        while (state != null)
        {
            // If state is dead, i.e. has a reference count of zero. Remove the files in it which is not in the alive set.
            if (state.Unreference())
            {
                foreach (FileId fileId in state.Files)
                {
                    if (!alive.Contains(fileId))
                    {
                        dead.Add(fileId);
                    }
                }

                // Remove the state from the state linked-list.
                ManifestState? next = state.Next;
                ManifestState? prev = state.Previous;

                if (prev != null)
                {
                    prev.Next = next;
                }

                if (next != null)
                {
                    next.Previous = prev;
                }
            }

            state = state.Previous;
        }

        // Remove all files which were marked dead.
        foreach (FileId fileId in dead)
        {
            string path = fileId.GetPath(Path);

            _fs.DeleteFile(path);
        }
    }

    private void Recover()
    {
        FileId? latestManifest = default;

        foreach (string path in _fs.ListFiles(Path))
        {
            if (FileId.TryParse(path, out FileId fileId) && fileId.Type == FileType.Manifest && (latestManifest == null || latestManifest.Value.Number < fileId.Number))
            {
                latestManifest = fileId;
            }
        }

        // If manifest does not exist, create an empty initial manifest state.
        if (latestManifest == null)
        {
            Current = new ManifestState(versionNo: 0, fileNo: 0, ImmutableArray<FileId>.Empty);

            return;
        }

        // TODO: Implement recovery.
        Current = new ManifestState(versionNo: 0, fileNo: 0, ImmutableArray<FileId>.Empty);
    }

    private void LogEdit(in ManifestEdit edit)
    {
        EncodeUInt64(edit.VersionNumber);
        EncodeUInt64(edit.FileNumber);
        EncodeFileIdArray(edit.FilesUnregistered);
        EncodeFileIdArray(edit.FilesRegistered);

        _writer.Flush();

        void EncodeUInt64(ulong? value)
        {
            _writer.Write(value.HasValue);

            if (value.HasValue)
            {
                _writer.Write7BitEncodedInt64((long)value.Value);
            }
        }

        void EncodeFileIdArray(FileId[]? value)
        {
            value ??= Array.Empty<FileId>();

            _writer.Write7BitEncodedInt(value.Length);

            foreach (FileId fileId in value)
            {
                _writer.Write((byte)fileId.Type);
                _writer.Write7BitEncodedInt64((long)fileId.Number);
            }
        }
    }

    // TODO: Move state to a new squashed manfiest log?
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        foreach (File file in _cache.Values)
        {
            if (file is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        // Clean up unused files.
        Clean();

        _writer.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
