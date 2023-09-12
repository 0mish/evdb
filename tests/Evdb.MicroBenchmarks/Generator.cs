namespace Evdb.MicroBenchmarks;

public static class Generator
{
    public static List<KeyValuePair<byte[], byte[]>> KeyValues(int count, int keySize = 12, int valueSize = 64)
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

        return kvs.ToList();
    }
}
