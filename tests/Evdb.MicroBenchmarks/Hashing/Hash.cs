using BenchmarkDotNet.Attributes;

namespace Evdb.MicroBenchmarks.Hashing;

public class Hash
{
    private byte[] _data = default!;

    [Params(1024)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = new byte[N];

        Random.Shared.NextBytes(_data);
    }

    [Benchmark]
    public uint Murmur1()
    {
        return Evdb.Hashing.Murmur1.Compute(_data).Value;
    }
}
