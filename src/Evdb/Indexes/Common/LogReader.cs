using Evdb.IO;
using System.Text;

namespace Evdb.Indexes.Common;

// TODO: Implement scrolling memory mapped buffer.
public sealed class LogReader
{
    private bool _disposed;
    private readonly Stream _file;
    private readonly BinaryReader _reader;

    public LogReader(IFileSystem fs, string path)
    {
        ArgumentNullException.ThrowIfNull(fs, nameof(fs));
        ArgumentNullException.ThrowIfNull(path, nameof(path));

        _file = fs.OpenFile(path, FileMode.Open, FileAccess.Read);
        _reader = new BinaryReader(_file, Encoding.UTF8, leaveOpen: true);
    }

    public bool Read(out Span<byte> data)
    {
        long length = _reader.ReadInt64();
        int checksum = _reader.ReadInt32();
        byte[] data0 = _reader.ReadBytes((int)length);

        if (Crc32.Compute(data0).Value != checksum)
        {
            data = default;

            return false;
        }

        data = data0;

        return true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _reader.Dispose();
        _file.Dispose();

        _disposed = true;
    }
}
