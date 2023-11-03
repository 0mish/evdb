namespace Evdb.Benchmarks;

public interface IBenchmarkDriver
{
    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    public void WaitCompaction();
}
