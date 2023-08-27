namespace Evdb.Hashing;

internal struct Crc32c
{
    public int Value { get; }

    public Crc32c Extend(in ReadOnlySpan<byte> data)
    {
        return default;
    }

    public static Crc32c Compute(in ReadOnlySpan<byte> data)
    {
        return default;
    }
}
