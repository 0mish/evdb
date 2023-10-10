using Evdb.IO;
using System.Diagnostics;
using System.Text;

namespace Evdb.Indexing;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class PhysicalLog : File, IDisposable
{
    private string DebuggerDisplay => $"PhysicalLog {Metadata.Path}";

    private bool _disposed;

    private readonly Stream _file;
    private readonly BinaryWriter _writer;

    public PhysicalLog(IFileSystem fs, FileMetadata metadata) : base(metadata)
    {
        _file = fs.OpenFile(metadata.Path, FileMode.Create, FileAccess.Write, FileShare.None);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);
    }

    public void LogSet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        _writer.Write((byte)1);
        _writer.Write7BitEncodedInt(key.Length);
        _writer.Write(key);
        _writer.Write7BitEncodedInt(value.Length);
        _writer.Write(value);
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
