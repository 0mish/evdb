using Evdb.Storage.LogsDb.Format;
using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Storage.LogsDb;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalLog : File, IDisposable
{
    private string DebuggerDisplay => $"{nameof(PhysicalLog)} = {Metadata.Path}";

    private bool _disposed;
    private BinaryEncoder _encoder;
    private LogWriter? _log;

    private readonly IFileSystem _fs;

    public PhysicalLog(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        _fs = fs;
        _encoder = new BinaryEncoder(Array.Empty<byte>());
    }

    public void Open()
    {
        FileStream file = _fs.OpenFile(Metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None);

        _log = new LogWriter(file);
    }

    public Status LogSet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_disposed)
        {
            return Status.Disposed;
        }
        else if (_log == null)
        {
            return Status.Closed;
        }

        _encoder.ByteArray(key);
        _encoder.ByteArray(value);

        _log?.Write(_encoder.Span);

        _encoder.Reset();

        return Status.Success;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _log?.Dispose();
        _log = null;

        _disposed = true;
    }
}
