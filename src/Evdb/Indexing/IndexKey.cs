using System.Buffers.Binary;

namespace Evdb.Indexing;

internal readonly ref struct IndexKey
{
    public ReadOnlySpan<byte> FullKey { get; }
    public ReadOnlySpan<byte> Key => FullKey.Slice(0, FullKey.Length - sizeof(ulong));
    public ulong Version => BinaryPrimitives.ReadUInt64LittleEndian(FullKey.Slice(FullKey.Length - sizeof(ulong)));

    public IndexKey(ReadOnlySpan<byte> value)
    {
        FullKey = value;
    }

    public int CompareTo(IndexKey other)
    {
        return FullKey.SequenceCompareTo(other.FullKey);
    }
}
