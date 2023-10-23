using BenchmarkDotNet.Attributes;
using Evdb.Collections;
using Evdb.Indexing.Format;

namespace Evdb.MicroBenchmarks.Iterators;

public class MoveTo
{
    private byte[] _key = default!;
    private SkipList _sl = default!;
    private Block _blk = default!;
    private List<KeyValuePair<byte[], byte[]>> _kvs = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _kvs = Generator.KeyValues(N);
        _sl = new SkipList();

        foreach (KeyValuePair<byte[], byte[]> kv in _kvs)
        {
            _sl.Set(kv.Key, kv.Value);
        }

        BlockBuilder builder = new();
        SkipList.Iterator iter = _sl.GetIterator();
        ReadOnlySpan<byte> lastKey = default;

        for (iter.MoveToFirst(); iter.IsValid; iter.MoveNext())
        {
            builder.Add(iter.Key, iter.Value);
            lastKey = iter.Key;
        }

        _key = lastKey.ToArray();
        _blk = new Block(builder.Span.ToArray());
    }

    [Benchmark]
    public void SkipList()
    {
        SkipList.Iterator iter = _sl.GetIterator();

        iter.MoveTo(_key);
    }

    [Benchmark]
    public void Block()
    {
        Block.Iterator iter = _blk.GetIterator();

        iter.MoveTo(_key);
    }
}
