using Evdb.IO;
using System.Text;

namespace Evdb.Indexes.Common;

// TODO: Implement scrolling memory mapped buffer.
public sealed class LogWriter : IDisposable
{
    private bool _disposed;
    private readonly Stream _file;
    private readonly BinaryWriter _writer;

    public LogWriter(IFileSystem fs, string path)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));
        ArgumentNullException.ThrowIfNull(path, nameof(path));

        _file = fs.OpenFile(path, FileMode.Create, FileAccess.Write);
        _writer = new BinaryWriter(_file, Encoding.UTF8, leaveOpen: true);
    }

    public void Write(in ReadOnlySpan<byte> data)
    {
        Crc32 checksum = Crc32.Compute(data);

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
