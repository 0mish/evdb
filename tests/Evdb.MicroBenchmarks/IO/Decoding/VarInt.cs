using BenchmarkDotNet.Attributes;
using LogsDb.IO;

namespace Evdb.MicroBenchmarks.IO.Decoding;

public class VarInt
{
    private BinaryReader _reader = default!;
    private BinaryDecoder _decoder;
    private byte[] _data1 = default!;
    private byte[] _data10 = default!;

    [Params(10000)]
    public int N;

    [Params(1, 10)]
    public int K;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _data1 = new byte[1 * N];
        _data10 = new byte[10 * N];

        BinaryEncoder e1 = new(_data1);
        BinaryEncoder e10 = new(_data10);

        for (int i = 0; i < N; i++)
        {
            e1.VarUInt64(1);
            e10.VarUInt64(ulong.MaxValue);
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _reader = new BinaryReader(new MemoryStream(K == 1 ? _data1 : _data10));
        _decoder = new BinaryDecoder(K == 1 ? _data1 : _data10);
    }

    [Benchmark(Baseline = true)]
    public void BinaryEncoder()
    {
        for (int i = 0; i < N; i++)
        {
            _decoder.VarUInt64(out _);
        }
    }

    [Benchmark]
    public void BinaryWriter()
    {
        for (int i = 0; i < N; i++)
        {
            _reader.Read7BitEncodedInt64();
        }
    }
}
