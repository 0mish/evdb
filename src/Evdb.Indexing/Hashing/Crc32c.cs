namespace Evdb.Hashing;

internal readonly struct Crc32c
{
    public uint Value { get; }

    public Crc32c(uint value)
    {
        Value = value;
    }

    public readonly Crc32c Extend(in ReadOnlySpan<byte> data)
    {
        uint crc = Value;

        for (int i = 0; i < data.Length; i++)
        {
            crc ^= data[i];

            for (int j = 0; j < 8; j++)
            {
                uint mask = (uint)(-(crc & 1));

                crc = (crc >> 1) ^ (0x82F63B78 & mask);
            }
        }

        return new Crc32c(~crc);
    }

    public static Crc32c Compute(in ReadOnlySpan<byte> data)
    {
        return new Crc32c(uint.MaxValue).Extend(data);
    }
}
