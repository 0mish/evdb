using BenchmarkDotNet.Attributes;
using Evdb.Collections;

namespace Evdb.MicroBenchmarks.Collections;

public class TryGetFalseBenchmark
{
    private SkipList _sl = default!;
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;
    private List<KeyValuePair<byte[], byte[]>> _nkvs = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _sl = new SkipList();
        _kvs = Generator.KeyValues(N * 2);
        _nkvs = _kvs.Skip(N).ToList();

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs.Take(N))
        {
            _sl.Set(kv.Key, kv.Value);
        }
    }

    [Benchmark]
    public bool SkipList()
    {
        bool result = false;

        foreach (var kv in _nkvs)
        {
            result |= _sl.TryGet(kv.Key, out _);
        }

        return result;
    }
}
