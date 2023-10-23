namespace Evdb.Collections;

internal sealed class BloomFilter
{
    private readonly byte[] _filter;

    // TODO: Hide this behind a Flush or Write method?
    public ReadOnlySpan<byte> Span => _filter;

    public BloomFilter(int size)
    {
        _filter = new byte[size];
    }

    public BloomFilter(byte[] filter)
    {
        ArgumentNullException.ThrowIfNull(filter, nameof(filter));

        _filter = filter;
    }

    public void Set(ReadOnlySpan<byte> key)
    {
        ulong[] hashes = Hash(key);

        foreach (ulong hash in hashes)
        {
            uint index = (uint)(hash % (uint)_filter.Length) / 8;
            uint bit = (uint)(hash % (uint)_filter.Length) % 8;

            _filter[index] |= (byte)(1 << (int)bit);
        }
    }

    public bool Test(ReadOnlySpan<byte> key)
    {
        ulong[] hashes = Hash(key);

        foreach (ulong hash in hashes)
        {
            uint index = (uint)(hash % (uint)_filter.Length) / 8;
            uint bit = (uint)(hash % (uint)_filter.Length) % 8;

            if ((_filter[index] & (byte)(1 << (int)bit)) == 0)
            {
                return false;
            }
        }

        return true;
    }

    private static ulong[] Hash(ReadOnlySpan<byte> key)
    {
        ulong[] hashes = new ulong[4];

        for (int i = 0; i < hashes.Length; i++)
        {
            hashes[i] = Fnv1(key, i);
        }

        return hashes;
    }

    // TODO: Consider moving this out to a Evdb.Hashing namespace.
    private static ulong Fnv1(ReadOnlySpan<byte> key, int seed)
    {
        const ulong FnvPrime = 0x00000100000001B3;
        const ulong FnvOffsetBasis = 0xCBF29CE484222325;

        ulong hash = FnvOffsetBasis ^ (ulong)(seed * 0x77);

        for (int i = 0; i < key.Length; i++)
        {
            hash ^= key[i];
            hash *= FnvPrime;
        }

        return hash;
    }
}
