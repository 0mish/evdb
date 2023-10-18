using BenchmarkDotNet.Attributes;
using Evdb.Collections;

namespace Evdb.MicroBenchmarks.Collections;

public class TryGetTrue
{
    private SkipList _sl = default!;
    private SortedDictionary<byte[], byte[]> _sd = default!;
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _kvs = Generator.KeyValues(N);

        _sl = new SkipList();
        _sd = new SortedDictionary<byte[], byte[]>(ByteArrayComparer.Default);

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            _sl.Set(kv.Key, kv.Value);
            _sd.Add(kv.Key, kv.Value);
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

    [Benchmark]
    public bool SortedDictionary()
    {
        bool result = true;

        foreach (var kv in _kvs)
        {
            result &= _sd.TryGetValue(kv.Key, out _);
        }

        return result;
    }
}
