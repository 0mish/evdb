using BenchmarkDotNet.Attributes;
using Evdb.Collections;

namespace Evdb.MicroBenchmarks.Collections;

public class TryGetTrueBenchmark
{
    private SkipList _sl = default!;
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _sl = new SkipList();
        _kvs = Generator.KeyValues(N);

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            _sl.Set(kv.Key, kv.Value);
        }
    }

    [Benchmark]
    public bool SkipList()
    {
        bool result = true;

        foreach (var kv in _kvs)
        {
            result &= _sl.TryGet(kv.Key, out _);
        }

        return result;
    }
}
