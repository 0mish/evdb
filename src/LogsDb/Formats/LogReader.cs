using System.Text;

namespace LogsDb.Formats;

internal sealed class LogReader : IDisposable
{
    private bool _disposed;
    private readonly FileStream _stream;
    private readonly BinaryReader _reader;

    public LogReader(FileStream stream)
    {
        _stream = stream;
        _reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
    }

    public Status Read(out byte[]? data)
    {
        data = null;

        if (_stream.Position >= _stream.Length)
        {
            return Status.EoF;
        }

        int length = _reader.Read7BitEncodedInt();

        data = _reader.ReadBytes(length);

        return Status.Success;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _reader.Dispose();
        _stream.Dispose();

        _disposed = true;
    }
}
