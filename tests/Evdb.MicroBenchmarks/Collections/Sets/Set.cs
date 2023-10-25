using BenchmarkDotNet.Attributes;
using Evdb.Collections;

namespace Evdb.MicroBenchmarks.Collections.Sets;

public class Set
{
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _kvs = Generator.KeyValues(N);
    }

    [Benchmark]
    public void BloomFilter()
    {
        BloomFilter bloom = new(new byte[4096]);

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            bloom.Set(kv.Key);
        }
    }
}
