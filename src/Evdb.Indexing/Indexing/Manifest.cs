using Evdb.IO;
using System.Text;

namespace Evdb.Indexing;

internal sealed class Manifest : IDisposable
{
    private bool _disposed;

    private ulong _versionNumber;
    private ulong _fileNumber;
    private ManifestState _current = default!;

    private readonly Stream _file;
    private readonly BinaryWriter _writer;
    private readonly IFileSystem _fs;

    public string Path { get; }
    public ulong VersionNumber => _versionNumber;
    public ulong FileNumber => _fileNumber;
    public ManifestState Current => _current;

    public Manifest(IFileSystem fs, string path)
    {
        _fs = fs;
        _fs.CreateDirectory(path);

        Path = path;

        // TODO: Locate latest valid manifest file.
        FileId id = new(FileType.Manifest, number: 0);

        Recover();

        _file = fs.OpenFile(id.GetPath(path), FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);
    }

    public ulong NextVersionNumber()
    {
        return _versionNumber++;
    }

    public ulong NextFileNumber()
    {
        return _fileNumber++;
    }

    public void Commit(ManifestEdit edit)
    {
        if (_disposed)
        {
            return;
        }

        ManifestState oldState;
        ManifestState newState;

        do
        {
            oldState = _current;

            List<VirtualTable> vtables = new(oldState.VirtualTables);
            List<PhysicalTable> ptables = new(oldState.PhysicalTables);
            List<PhysicalLog> plogs = new(oldState.PhysicalLogs);

            foreach (object obj in edit.Unregistered ?? Array.Empty<object>())
            {
                switch (obj)
                {
                    case VirtualTable vtable: vtables.Remove(vtable); break;
                    case PhysicalTable ptable: ptables.Remove(ptable); break;
                    case PhysicalLog plog: plogs.Remove(plog); break;
                }
            }

            foreach (object obj in edit.Registered ?? Array.Empty<object>())
            {
                switch (obj)
                {
                    case VirtualTable vtable: vtables.Add(vtable); break;
                    case PhysicalTable ptable: ptables.Add(ptable); break;
                    case PhysicalLog plog: plogs.Add(plog); break;
                }
            }

            newState = new ManifestState(vtables.ToArray(), ptables.ToArray(), plogs.ToArray());
        }
        while (Interlocked.CompareExchange(ref _current, newState, oldState) != oldState);

        // FIXME: Log the edit.
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
            _current = new ManifestState(Array.Empty<VirtualTable>(), Array.Empty<PhysicalTable>(), Array.Empty<PhysicalLog>());

            return;
        }

        // TODO: Implement recovery.
        _current = new ManifestState(Array.Empty<VirtualTable>(), Array.Empty<PhysicalTable>(), Array.Empty<PhysicalLog>());
    }

#if false
    private void LogEdit(in ManifestEdit edit, ulong versionNumber, ulong fileNumber)
    {
        EncodeUInt64(versionNumber);
        EncodeUInt64(fileNumber);

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
#endif

    // TODO: Move state to a new squashed manfiest log?
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // FIXME: Log latest version?

        _writer.Dispose();
        _file.Dispose();

        foreach (PhysicalLog log in Current.PhysicalLogs)
        {
            log.Dispose();
        }

        foreach (PhysicalTable table in Current.PhysicalTables)
        {
            table.Dispose();
        }

        foreach (VirtualTable table in Current.VirtualTables)
        {
            table.Dispose();
        }

        _disposed = true;
    }
}
