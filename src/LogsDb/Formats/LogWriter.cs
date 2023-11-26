using LogsDb.IO;

namespace LogsDb.Formats;

internal sealed class LogWriter : IDisposable
{
    private bool _disposed;
    private BinaryEncoder _encoder;
    private readonly FileStream _stream;

    public ulong Length => (ulong)_stream.Length;

    public LogWriter(FileStream stream)
    {
        _encoder = new BinaryEncoder();
        _stream = stream;
    }

    public Status Write(ReadOnlySpan<byte> data)
    {
        // TODO: CRC32 checksum and error checking.
        _encoder.ByteArray(data);

        _stream.Write(_encoder.Span);

        _encoder.Reset();

        return Status.Success;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _stream.Dispose();

        _disposed = true;
    }
}
