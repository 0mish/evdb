using Evdb.IO;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Text;

namespace Evdb.Indexes.Lsm;

public sealed class Manifest : IDisposable
{
    private bool _disposed;

    private ulong _versionNumber;
    private ulong _fileNumber;

    private readonly object _sync;
    private readonly FileStream _file;
    private readonly BinaryWriter _writer;

    public ulong VersionNumber => _versionNumber;
    public ulong FileNumber => _fileNumber;
    public ManifestRevision Current { get; private set; }

    public Manifest(IFileSystem fs)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));

        // TODO: Locate latest valid manifest file.
        FileMetadata metadata = new(FileType.Manifest, number: 0);

        _sync = new object();
        _file = fs.OpenFile(metadata.Path, FileMode.OpenOrCreate, FileAccess.Write);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);

        // TODO: Load the current revision from disk.
        Current = new ManifestRevision(versionNo: 0, fileNo: 0, ImmutableArray<FileMetadata>.Empty);
    }

    public ulong NextVersionNumber()
    {
        return _versionNumber++;
    }

    public ulong NextFileNumber()
    {
        return _fileNumber++;
    }

    public void Commit(in ManifestEdit edit)
    {
        lock (_sync)
        {
            ulong versionNo = edit.VersionNumber ?? VersionNumber;
            ulong fileNo = edit.FileNumber ?? FileNumber;

            Debug.Assert(Current.VersionNumber <= versionNo, "Committing an older version number than current.");
            Debug.Assert(Current.FileNumber <= fileNo, "Committing an older file number than current.");

            List<FileMetadata> files = new(Current.Files);

#if false
            if (edit.FilesUnregistered != null)
            {
                foreach (FileId fileId in edit.FilesUnregistered)
                {
                    files.Remove(fileId);
                }
            }

            if (edit.FilesRegistered != null)
            {
                files.AddRange(edit.FilesRegistered);
            }
#endif

            // Append the new revision to the front of the list.
            ManifestRevision @new = new(versionNo, fileNo, files.ToImmutableArray());

            Current.Next = @new;
            @new.Previous = Current;

            Current = @new;

            // TODO:
            //
            // Verify that LogEdit cannot trigger a flush to disk. We expect that only _writer.Flush() & related
            // disposes do so.
            LogEdit(edit);
        }

        _writer.Flush();
    }

    // TODO: Implement delta encoding.
    private void LogEdit(in ManifestEdit edit)
    {
        _writer.Write(edit.VersionNumber.HasValue);

        if (edit.VersionNumber.HasValue)
        {
            _writer.Write7BitEncodedInt64((long)edit.VersionNumber.Value);
        }

        _writer.Write(edit.FileNumber.HasValue);

        if (edit.FileNumber.HasValue)
        {
            _writer.Write7BitEncodedInt64((long)edit.FileNumber.Value);
        }

        if (edit.FilesRegistered == null)
        {
            _writer.Write7BitEncodedInt(0);
        }
        else
        {
            _writer.Write7BitEncodedInt(edit.FilesRegistered.Count);

            foreach (FileId fileId in edit.FilesRegistered)
            {
                _writer.Write7BitEncodedInt((int)fileId.Type);
                _writer.Write7BitEncodedInt64((long)fileId.Number);
            }
        }

        if (edit.FilesUnregistered == null)
        {
            _writer.Write7BitEncodedInt(0);
        }
        else
        {
            _writer.Write7BitEncodedInt(edit.FilesUnregistered.Count);

            foreach (FileId fileId in edit.FilesUnregistered)
            {
                _writer.Write7BitEncodedInt((int)fileId.Type);
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

        _writer.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
