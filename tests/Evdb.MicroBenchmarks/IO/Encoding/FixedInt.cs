using BenchmarkDotNet.Attributes;
using Evdb.IO;

namespace Evdb.MicroBenchmarks.IO.Encoding;

public class FixedInt
{
    private BinaryWriter _writer = default!;
    private BinaryEncoder _encoder;

    [Params(10000)]
    public int N;

    [IterationSetup]
    public void IterationSetup()
    {
        _writer = new BinaryWriter(new MemoryStream());
        _encoder = new BinaryEncoder(Array.Empty<byte>());
    }

    [Benchmark(Baseline = true)]
    public void BinaryEncoder()
    {
        for (int i = 0; i < N; i++)
        {
            _encoder.UInt32(uint.MaxValue);
        }
    }

    [Benchmark]
    public void BinaryWriter()
    {
        for (int i = 0; i < N; i++)
        {
            _writer.Write(uint.MaxValue);
        }
    }
}
