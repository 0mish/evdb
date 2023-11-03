namespace Evdb.Benchmarks;

public record class BenchmarkResult(string Name, ulong BytesWritten, ulong BytesRead, ulong Misses, TimeSpan WriteDuration, TimeSpan ReadDuration);
