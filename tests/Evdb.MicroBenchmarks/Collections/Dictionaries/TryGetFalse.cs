using BenchmarkDotNet.Attributes;
using LogsDb.Collections;

namespace Evdb.MicroBenchmarks.Collections.Dictionaries;

public class TryGetFalse
{
    private SkipList _sl = default!;
    private SortedDictionary<byte[], byte[]> _sd = default!;

    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;
    private List<KeyValuePair<byte[], byte[]>> _nkvs = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _kvs = Generator.KeyValues(N * 2);
        _nkvs = _kvs.Skip(N).ToList();

        _sl = new SkipList();
        _sd = new SortedDictionary<byte[], byte[]>(ByteArrayComparer.Default);

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs.Take(N))
        {
            _sl.Set(kv.Key, kv.Value);
            _sd.Add(kv.Key, kv.Value);
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _sl.Dispose();
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

    [Benchmark]
    public bool SortedDictionary()
    {
        bool result = false;

        foreach (var kv in _nkvs)
        {
            result |= _sl.TryGet(kv.Key, out _);
        }

        return result;
    }
}
