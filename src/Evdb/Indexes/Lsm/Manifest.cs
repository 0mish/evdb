using Evdb.IO;
using Evdb.Threading;
using System.Collections.Immutable;
using System.Text;

namespace Evdb.Indexes.Lsm;

public sealed class Manifest : IDisposable
{
    private bool _disposed;

    private ulong _versionNumber;
    private ulong _fileNumber;

    private readonly object _sync;
    private readonly Stream _file;
    private readonly BinaryWriter _writer;
    private readonly IFileSystem _fs;

    public string Path { get; }
    public ulong VersionNumber => _versionNumber;
    public ulong FileNumber => _fileNumber;
    public ManifestState Current { get; private set; }

    public Manifest(IFileSystem fs, string path, object sync)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));
        ArgumentNullException.ThrowIfNull(path, nameof(path));
        ArgumentNullException.ThrowIfNull(sync, nameof(sync));

        _sync = sync;

        _fs = fs;
        _fs.CreateDirectory(path);

        Path = path;

        // TODO: Locate latest valid manifest file.
        FileId id = new(FileType.Manifest, number: 0);

        Recover();

        _file = fs.OpenFile(id.GetPath(path), FileMode.OpenOrCreate, FileAccess.Write);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);

        // TODO: Load the current revision from disk.
        Current = new ManifestState(versionNo: 0, fileNo: 0, ImmutableArray<FileId>.Empty);
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

        if (latestManifest == null)
        {
            return;
        }

        // TODO: Implement recovery.
    }

    public ulong NextVersionNumber()
    {
        return _versionNumber++;
    }

    public ulong NextFileNumber()
    {
        return _fileNumber++;
    }

    public FileMetadata? Resolve(FileId fileId)
    {
        return null;
    }

    public void Commit(ManifestEdit edit)
    {
        // FIXME: What is up here?
        ulong versionNo = edit.VersionNumber ?? VersionNumber;
        ulong fileNo = edit.FileNumber ?? FileNumber;

        edit.VersionNumber ??= VersionNumber;
        edit.FileNumber ??= FileNumber;

        List<FileId> files = new(Current.Files);

        if (edit.FilesUnregistered != null)
        {
            foreach (FileId fileId in edit.FilesUnregistered)
            {
                files.Remove(fileId);
            }
        }

        if (edit.FilesRegistered != null)
        {
            foreach (FileId fileId in edit.FilesRegistered)
            {
                files.Add(fileId);
            }
        }

        // Append the new state to the front of the state list.
        ManifestState @new = new(versionNo, fileNo, files.ToImmutableArray());
        Current.Next = @new;
        @new.Previous = Current;
        Current = @new;

        bool lockReleased = false;

        try
        {
            MonitorHelper.Exit(_sync, out lockReleased);

            // TODO:
            // Review if this is safe from corruption when mulitple threads are here. Do we even multiple theads to be here?
            LogEdit(edit);
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
            if (value == null)
            {
                _writer.Write7BitEncodedInt(0);

                return;
            }

            _writer.Write7BitEncodedInt(value.Length);

            foreach (FileId fileId in value)
            {
                _writer.Write((byte)fileId.Type);
                _writer.Write7BitEncodedInt64((long)fileId.Number);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // TODO: Commit the edit to disk?

        // Clean up unused files.
        Clean();

        _writer.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
