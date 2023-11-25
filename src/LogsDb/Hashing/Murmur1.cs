using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LogsDb.Hashing;

internal readonly struct Murmur1
{
    public uint Value { get; }

    public Murmur1(uint value)
    {
        Value = value;
    }

    public static Murmur1 Compute(ReadOnlySpan<byte> data, uint seed = 0xf31ad049)
    {
        const uint M = 0xc6a4a793;
        const int R = 16;

        uint n = (uint)data.Length;
        uint h = seed ^ n * M;

        ref byte rbyte = ref MemoryMarshal.GetReference(data);
        ref uint rint = ref Unsafe.As<byte, uint>(ref rbyte);

        while (n >= 4)
        {
            // FIXME: Endianness.
            uint k = rint;

            h += k;
            h *= M;
            h ^= h >> R;

            n -= 4;
            rint = ref Unsafe.Add(ref rint, 1);
        }

        rbyte = ref Unsafe.As<uint, byte>(ref rint);

        switch (n)
        {
            case 3:
                h += (uint)Unsafe.Add(ref rbyte, 2) << 16;
                goto case 2;
            case 2:
                h += (uint)Unsafe.Add(ref rbyte, 1) << 8;
                goto case 1;
            case 1:
                h += rbyte;
                h *= M;
                h ^= h >> R;
                break;
        }

        h *= M;
        h ^= h >> 10;
        h *= M;
        h ^= h >> 17;

        return new Murmur1(h);
    }
}
