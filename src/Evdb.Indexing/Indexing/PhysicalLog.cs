using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalLog : File, IDisposable
{
    private string DebuggerDisplay => $"{nameof(PhysicalLog)} = {Metadata.Path}";

    private bool _disposed;
    private BinaryEncoder _encoder;
    private FileStream? _file;

    private readonly IFileSystem _fs;

    public PhysicalLog(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        _fs = fs;
        _encoder = new BinaryEncoder(Array.Empty<byte>());
    }

    public void Open()
    {
        _file = _fs.OpenFile(Metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None);
    }

    public void LogSet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        _encoder.ByteArray(key);
        _encoder.ByteArray(value);

        _file!.Write(_encoder.Span);

        _encoder.Reset();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _file?.Dispose();

        _disposed = true;
    }
}
