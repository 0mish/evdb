using Evdb.IO;
using System.Text;

namespace Evdb.Indexing.Format;

internal sealed class BlockBuilder : IDisposable
{
    private bool _disposed;
    private readonly BinaryWriter _writer;

    public Stream BaseStream { get; }
    public ulong Length { get; private set; }

    public BlockBuilder(Stream stream, bool leaveOpen)
    {
        BaseStream = stream;

        _writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen);
    }

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ulong position = (ulong)_writer.BaseStream.Position;

        _writer.WriteByteArray(key);
        _writer.WriteByteArray(value);

        Length += (ulong)_writer.BaseStream.Position - position;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _writer.Dispose();

        _disposed = true;
    }
}
