﻿using System.Diagnostics.CodeAnalysis;

namespace Evdb.Tests;

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

public static class Generator
{
    public static Dictionary<byte[], byte[]> RandomKeyValues(int count, int keySize = 12, int valueSize = 64)
    {
        Random random = new(Seed: 0);
        Dictionary<byte[], byte[]> kvs = new(new ByteArrayComparer());

        byte[] key = new byte[keySize];
        byte[] value = new byte[valueSize];

        while (kvs.Count < count)
        {
            random.NextBytes(key);
            random.NextBytes(value);

            kvs[key.ToArray()] = value.ToArray();
        }

        return kvs;
    }
}
