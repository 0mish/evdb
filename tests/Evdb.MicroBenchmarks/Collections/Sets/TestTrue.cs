using BenchmarkDotNet.Attributes;
using LogsDb.Collections;

namespace Evdb.MicroBenchmarks.Collections.Sets;

public class TestTrue
{
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;
    private BloomFilter _bloom = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bloom = new(new byte[4096]);
        _kvs = Generator.KeyValues(N);

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            _bloom.Set(kv.Key);
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
}
