using LogsDb.Hashing;

namespace LogsDb.Collections;

internal sealed class BloomFilter
{
    private readonly uint _size;
    private readonly byte[] _filter;

    public ReadOnlySpan<byte> Span => _filter;

    public BloomFilter(byte[] filter)
    {
        _filter = filter;
        _size = (uint)filter.Length;
    }

    public void Set(ReadOnlySpan<byte> key)
    {
        uint h = Hash(key);
        uint d = h >> 15 | h << 17;

        for (int i = 0; i < 4; i++)
        {
            uint index = h % _size / 8;
            uint bit = h % _size % 8;

            _filter[index] |= (byte)(1 << (int)bit);

            h += d;
        }
    }

    public bool Test(ReadOnlySpan<byte> key)
    {
        uint h = Hash(key);
        uint d = h >> 15 | h << 17;

        for (int i = 0; i < 4; i++)
        {
            uint index = h % _size / 8;
            uint bit = h % _size % 8;

            if ((_filter[index] & (byte)(1 << (int)bit)) == 0)
            {
                return false;
            }

            h += d;
        }

        return true;
    }

    private static uint Hash(ReadOnlySpan<byte> key)
    {
        return Murmur1.Compute(key).Value;
    }
}
