using System.Buffers.Binary;

namespace Evdb;

internal readonly ref struct RecordKey
{
    public ReadOnlySpan<byte> FullKey { get; }
    public ReadOnlySpan<byte> Key => FullKey.Slice(0, FullKey.Length - sizeof(int));
    public int Count => BinaryPrimitives.ReadInt32LittleEndian(FullKey.Slice(FullKey.Length - sizeof(int)));

    public static byte[] Encode(ReadOnlySpan<byte> key, int count)
    {
        byte[] result = new byte[key.Length + sizeof(uint)];

        key.CopyTo(result);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan().Slice(key.Length), count);

        return result;
    }
}
