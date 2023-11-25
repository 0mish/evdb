using BenchmarkDotNet.Attributes;
using Evdb.IO;

namespace Evdb.MicroBenchmarks.IO.Encoding;

public class VarInt
{
    private BinaryWriter _writer = default!;
    private BinaryEncoder _encoder;

    [Params(10000)]
    public int N;

    [IterationSetup]
    public void IterationSetup()
    {
        _writer = new BinaryWriter(new MemoryStream());
        _encoder = new BinaryEncoder();
    }

    [Benchmark(Baseline = true)]
    public void BinaryEncoder()
    {
        for (int i = 0; i < N; i++)
        {
            _encoder.VarUInt64(ulong.MaxValue);
        }
    }

    [Benchmark]
    public void BinaryWriter()
    {
        for (int i = 0; i < N; i++)
        {
            _writer.Write7BitEncodedInt64(long.MaxValue);
        }
    }
}
