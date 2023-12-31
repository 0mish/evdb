﻿using BenchmarkDotNet.Attributes;
using LogsDb.Collections;

namespace Evdb.MicroBenchmarks.Collections.Dictionaries;

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
    public void SkipList()
    {
        using SkipList sl = new();

        foreach (var kv in _kvs)
        {
            sl.Set(kv.Key, kv.Value);
        }
    }

    [Benchmark]
    public void SortedDictionary()
    {
        SortedDictionary<byte[], byte[]> sd = new(ByteArrayComparer.Default);

        foreach (var kv in _kvs)
        {
            sd.Add(kv.Key, kv.Value);
        }
    }
}
