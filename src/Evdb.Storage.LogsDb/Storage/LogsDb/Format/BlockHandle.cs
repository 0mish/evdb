using Evdb.IO;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;

namespace Evdb.Storage.LogsDb.Format;

internal struct BlockHandle : IEquatable<BlockHandle>
{
    public ulong Position { get; set; }
    public ulong Length { get; set; }

    public BlockHandle(ulong position, ulong length)
    {
        Position = position;
        Length = length;
    }

    public BlockHandle(ReadOnlySpan<byte> data)
    {
        Position = BinaryPrimitives.ReadUInt64LittleEndian(data);
        Length = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(sizeof(ulong)));
    }

    public readonly override int GetHashCode()
    {
        return HashCode.Combine(Position, Length);
    }

    public readonly override bool Equals([NotNullWhen(true)] object? obj)
    {
        return obj is BlockHandle handle && Equals(handle);
    }

    public readonly bool Equals(BlockHandle handle)
    {
        return Position == handle.Position && Length == handle.Length;
    }

    public readonly override string ToString()
    {
        return $"({Position}+{Length})";
    }

    public static byte[] Encode(BlockHandle handle)
    {
        byte[] result = new byte[sizeof(ulong) + sizeof(ulong)];
        Span<byte> span = result;

        BinaryPrimitives.WriteUInt64LittleEndian(span, handle.Position);
        BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(sizeof(ulong)), handle.Length);

        return result;
    }

    public static BlockHandle Read(ref BinaryDecoder reader)
    {
        reader.UInt64(out ulong position);
        reader.UInt64(out ulong length);

        return new BlockHandle
        {
            Position = position,
            Length = length
        };
    }
}
