using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalLog : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalLog {Metadata.Path}";

    private bool _disposed;
    private BinaryEncoder _encoder;
    private readonly FileStream _file;

    public PhysicalLog(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        _file = fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None);
        _encoder = new BinaryEncoder(Array.Empty<byte>());
    }

    public void LogSet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        _encoder.ByteArray(key);
        _encoder.ByteArray(value);

        _file.Write(_encoder.Span);

        _encoder.Reset();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _file.Dispose();

        _disposed = true;
    }
}
