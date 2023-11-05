using BenchmarkDotNet.Attributes;
using Evdb.Collections;

namespace Evdb.MicroBenchmarks.Collections.Sets;

public class TestTrue
{
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;
    private BloomFilter _bloom = default!;
    private BlockedBloomFilter _blockedBloom = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bloom = new BloomFilter(new byte[4096]);
        _blockedBloom = new BlockedBloomFilter(new byte[4096]);
        _kvs = Generator.KeyValues(N);

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            _bloom.Set(kv.Key);
            _blockedBloom.Set(kv.Key);
        }
    }

    [Benchmark]
    public bool BloomFilter()
    {
        bool result = true;

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            result &= _bloom.Test(kv.Key);
        }

        return result;
    }

    [Benchmark]
    public bool BlockedBloomFilter()
    {
        bool result = true;

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            result &= _blockedBloom.Test(kv.Key);
        }

        return result;
    }
}
