using Evdb.IO;
using System.Diagnostics;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalLog : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalLog {Metadata.Path}";

    private bool _disposed;
    private readonly FileStream _file;
    private readonly AppendFile _log;

    public PhysicalLog(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        _file = fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None);
        _log = new AppendFile(_file, bufferSize: 4 * 1024);
    }

    public void LogSet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        // TODO: Optimize this buffer allocation.
        BinaryEncoder encoder = new(Array.Empty<byte>());

        encoder.ByteArray(key);
        encoder.ByteArray(value);

        _log.Write(encoder.Span);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _log.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
