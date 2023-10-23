using Evdb.IO;

namespace Evdb.Indexing.Format;

internal struct BlockBuilder
{
    private BinaryEncoder _encoder;

    public readonly ReadOnlySpan<byte> Span => _encoder.Span;
    public readonly ulong Length => _encoder.Length;
    public readonly bool IsEmpty => _encoder.IsEmpty;

    public BlockBuilder()
    {
        _encoder = new BinaryEncoder(Array.Empty<byte>());
    }

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        // TODO: Implement restarts, prefix-truncation, prefix-compression.
        _encoder.ByteArray(key);
        _encoder.ByteArray(value);
    }

    public void Complete()
    {
        // TODO: Encode CRC32 checksum and other metadata.
    }

    public void Reset()
    {
        _encoder.Reset();
    }

    public readonly void CopyTo(Stream stream)
    {
        stream.Write(_encoder.Span);
    }
}
