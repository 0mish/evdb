using Evdb.Hashing;
using System.Diagnostics;

namespace Evdb.Collections;

internal class BlockedBloomFilter
{
    private const int CacheLineSize = 1 << CacheLineLogSize;
    private const int CacheLineLogSize = 6;

    private readonly uint _size;
    private readonly byte[] _filter;

    public ReadOnlySpan<byte> Span => _filter;

    public BlockedBloomFilter(byte[] filter)
    {
        Debug.Assert(filter.Length % CacheLineSize == 0);

        _filter = filter;
        _size = (uint)filter.Length;
    }

    public void Set(ReadOnlySpan<byte> key)
    {
        uint h = Hash(key);
        uint d = h >> 15 | h << 17;

        uint b = h % (_size / CacheLineSize) * CacheLineSize;

        for (int i = 0; i < 4; i++)
        {
            uint index = (h % CacheLineSize) >> 3;
            uint bit = (h % CacheLineSize) & 0x7;

            _filter[b + index] |= (byte)(1 << (int)bit);

            h += d;
        }
    }

    public bool Test(ReadOnlySpan<byte> key)
    {
        uint h = Hash(key);
        uint d = h >> 15 | h << 17;

        uint b = h % (_size / CacheLineSize) * CacheLineSize;

        for (int i = 0; i < 4; i++)
        {
            uint index = (h % CacheLineSize) >> 3;
            uint bit = (h % CacheLineSize) & 0x7;

            if ((_filter[b + index] & (byte)(1 << (int)bit)) == 0)
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
