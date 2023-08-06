namespace Evdb.Indexes.Common;

public struct Crc32
{
    public int Value { get; }

    private Crc32(int value)
    {
        Value = value;
    }

    public Crc32 Extend(in ReadOnlySpan<byte> data)
    {
        return default;
    }

    public static Crc32 Compute(in ReadOnlySpan<byte> data)
    {
        return default;
    }
}
