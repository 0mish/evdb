using Evdb.Hashing;
using Evdb.IO;
using System.Text;

namespace Evdb.Indexing;

// TODO: Implement scrolling memory mapped buffer.
internal sealed class LogWriter : IDisposable
{
    private bool _disposed;
    private readonly Stream _file;
    private readonly BinaryWriter _writer;

    public LogWriter(IFileSystem fs, string path)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));
        ArgumentNullException.ThrowIfNull(path, nameof(path));

        _file = fs.OpenFile(path, FileMode.Create, FileAccess.Write, FileShare.None);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);
    }

    public void Write(in ReadOnlySpan<byte> data)
    {
        Crc32c checksum = Crc32c.Compute(data);

        _writer.Write(data.Length);
        _writer.Write(checksum.Value);
        _writer.Write(data);
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
