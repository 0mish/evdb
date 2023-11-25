using BenchmarkDotNet.Attributes;
using LogsDb.IO;

namespace Evdb.MicroBenchmarks.IO.Encoding;

public class ByteArray
{
    private BinaryWriter _writer = default!;
    private BinaryEncoder _encoder;
    private byte[] _data = default!;

    [Params(10000)]
    public int N;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data = new byte[1024 * 16];
    }

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
            _encoder.ByteArray(_data);
            _encoder.Reset();
        }
    }

    [Benchmark]
    public void BinaryWriter()
    {
        for (int i = 0; i < N; i++)
        {
            _writer.Write7BitEncodedInt(_data.Length);
            _writer.Write(_data);
            _writer.Seek(0, SeekOrigin.Begin);
        }
    }
}
