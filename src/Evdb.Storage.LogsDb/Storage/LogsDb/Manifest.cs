using Evdb.Storage.LogsDb.Format;
using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Storage.LogsDb;

internal sealed class Manifest : IDisposable
{
    private bool _disposed;

    private ulong _fileNumber;
    private ManifestState _current;
    private LogWriter? _log;

    private readonly object _sync;
    private readonly IFileSystem _fs;
    private readonly IBlockCache _blockCache;

    public string Path { get; }
    public ulong FileNumber => _fileNumber;
    public ManifestState Current => _current;

    public Manifest(IFileSystem fs, string path, IBlockCache blockCache)
    {
        _fs = fs;
        _fs.CreateDirectory(path);

        _blockCache = blockCache;

        _sync = new object();
        _current = new ManifestState(Array.Empty<VirtualTable>(), Array.Empty<PhysicalTable>(), Array.Empty<PhysicalLog>());

        Path = path;
    }

    public ulong NextFileNumber()
    {
        return _fileNumber++;
    }

    public Status Open()
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

            return Open(new FileId(FileType.Manifest, number: 0));
        }

        // Otherwise we recover from the latest manifest.
        Status status = Recover(latestManifest.Value);

        if (!status.IsSuccess)
        {
            return status;
        }

        return Open(latestManifest.Value);
    }

    private Status Open(FileId id)
    {
        FileStream file = _fs.OpenFile(id.GetPath(Path), FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

        _log = new LogWriter(file);

        return Status.Success;
    }

    public Status Commit(in ManifestEdit edit)
    {
        if (_disposed)
        {
            return Status.Disposed;
        }
        else if (_log == null)
        {
            return Status.Closed;
        }

        // Serialize on the manifest so modifications written to the on disk and memory is in sync.
        //
        // TODO:
        // If we want to make this concurrent, we run into a problem similar to that of enabling concurrent writers on
        // the virtual tables. But the manifest is generally mutated at a slower rate compared to the virtual tables,
        // so not sure if worth the engineering effort. But is it worth the flex?
        lock (_sync)
        {
            ManifestState oldState = _current;

            List<VirtualTable> vtables = new(oldState.VirtualTables);
            List<PhysicalTable> ptables = new(oldState.PhysicalTables);
            List<PhysicalLog> plogs = new(oldState.PhysicalLogs);

            // Apply unregistrations first.
            CommitUnregistered(edit.VirtualTables, vtables);
            CommitUnregistered(edit.PhysicalTables, ptables);
            CommitUnregistered(edit.PhysicalLogs, plogs);

            // Apply registrations after.
            CommitRegistered(edit.VirtualTables, vtables);
            CommitRegistered(edit.PhysicalTables, ptables);
            CommitRegistered(edit.PhysicalLogs, plogs);

            _current = new ManifestState(vtables.ToArray(), ptables.ToArray(), plogs.ToArray());

            return Log(edit);
        }

        static void CommitUnregistered<T>(in ListEdit<T> edit, List<T> values)
        {
            if (edit.Unregistered == null)
            {
                return;
            }

            foreach (T value in edit.Unregistered)
            {
                values.Remove(value);
            }
        }

        static void CommitRegistered<T>(in ListEdit<T> edit, List<T> values)
        {
            if (edit.Registered != null)
            {
                values.AddRange(edit.Registered);
            }
        }
    }

    private Status Log(in ManifestEdit edit)
    {
        Debug.Assert(_log != null);
        Debug.Assert(Monitor.IsEntered(_sync));

        // If log exceeded maximum size, we segment and move to a new one.
        if (_log.Length > 1024)
        {
            Status status = Segment();

            if (!status.IsSuccess)
            {
                return status;
            }
        }

        BinaryEncoder encoder = new(Array.Empty<byte>());

        // Encode unregistrations first.
        Encode(edit.PhysicalTables.Unregistered);
        Encode(edit.PhysicalLogs.Unregistered);

        // Encode registrations after.
        Encode(edit.PhysicalTables.Registered);
        Encode(edit.PhysicalLogs.Registered);

        return _log.Write(encoder.Span);

        void Encode(File[]? files)
        {
            if (files == null)
            {
                encoder.VarInt32(0);
                return;
            }

            encoder.VarInt32(files.Length);

            foreach (File file in files)
            {
                FileId id = file.Metadata.Id;

                // FIXME: Encode as byte. But the range of id.Type is less than 128 so it is already encoded in a single byte.
                encoder.VarUInt32((byte)id.Type);
                encoder.VarUInt64(id.Number);
            }
        }
    }

    private Status Segment()
    {
        Debug.Assert(Monitor.IsEntered(_sync));

        // Close the current log.
        _log?.Dispose();

        ulong number = NextFileNumber();
        FileId id = new(FileType.Manifest, number);
        Status status = Open(id);

        if (!status.IsSuccess)
        {
            return status;
        }

        // Create the initial edit representing the current state.
        ManifestState state = _current;
        ManifestEdit edit = new(
            ptables: new ListEdit<PhysicalTable>(
                registered: state.PhysicalTables
            ),
            plogs: new ListEdit<PhysicalLog>(
                registered: state.PhysicalLogs
            )
        );

        return Log(edit);
    }

    private Status Recover(FileId id)
    {
        FileStream file = _fs.OpenFile(id.GetPath(Path), FileMode.Open, FileAccess.Read, FileShare.None);
        using LogReader reader = new(file);

        List<FileId> ptableIds = new();
        List<FileId> plogIds = new();

        while (true)
        {
            Status status = reader.Read(out byte[]? data);

            if (!status.IsSuccess || status.IsEoF)
            {
                break;
            }

            BinaryDecoder decoder = new(data!);

            if (!Decode(id => ptableIds.Remove(id)))
            {
                return Status.Corrupted;
            }

            if (!Decode(id => plogIds.Remove(id)))
            {
                return Status.Corrupted;
            }

            if (!Decode(id => ptableIds.Add(id)))
            {
                return Status.Corrupted;
            }

            if (!Decode(id => plogIds.Add(id)))
            {
                return Status.Corrupted;
            }

            bool Decode(Action<FileId> action)
            {
                if (!decoder.VarUInt32(out uint length))
                {
                    return false;
                }

                for (int i = 0; i < length; i++)
                {
                    if (!decoder.VarUInt32(out uint type))
                    {
                        return false;
                    }

                    if (!decoder.VarUInt64(out ulong number))
                    {
                        return false;
                    }

                    action(new FileId((FileType)type, number));
                }

                return true;
            }
        }

        ulong number = id.Number;
        List<PhysicalTable> ptables = new();
        List<PhysicalLog> plogs = new();

        foreach (FileId tid in ptableIds)
        {
            FileMetadata metadata = new(Path, tid.Type, tid.Number);
            PhysicalTable ptable = new(_fs, metadata, _blockCache);

            ptable.Open();
            ptables.Add(ptable);

            number = ulong.Max(tid.Number, number);
        }

        foreach (FileId lid in plogIds)
        {
            FileMetadata metadata = new(Path, lid.Type, lid.Number);
            PhysicalLog plog = new(_fs, metadata);

            plogs.Add(plog);

            number = ulong.Max(lid.Number, number);
        }

        _fileNumber = number;
        _current = new ManifestState(Array.Empty<VirtualTable>(), ptables.ToArray(), plogs.ToArray());

        return Status.Success;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // Move state to a new manifest log.
        lock (_sync)
        {
            Segment();

            _log?.Dispose();
        }

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
