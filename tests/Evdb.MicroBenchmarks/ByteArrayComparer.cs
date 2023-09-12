using System.Diagnostics.CodeAnalysis;

namespace Evdb.MicroBenchmarks;

public class ByteArrayComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
{
    public static ByteArrayComparer Default { get; } = new ByteArrayComparer();

    public int Compare(byte[]? x, byte[]? y)
    {
        return x.AsSpan().SequenceCompareTo(y);
    }

    public bool Equals(byte[]? x, byte[]? y)
    {
        return x.AsSpan().SequenceEqual(y);
    }

    public int GetHashCode([DisallowNull] byte[] obj)
    {
        int result = 0;

        foreach (byte b in obj)
        {
            HashCode.Combine(result, b);
        }

        return result;
    }
}
