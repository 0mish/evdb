namespace Evdb.Benchmarks;

public class BenchmarkOptions
{
    public int Entries { get; set; }
    public int WriterThreads { get; set; } = 1;
    public int ReaderThreads { get; set; } = 1;

    public int KeyLength { get; set; } = 12;
    public int ValueLength { get; set; } = 64;
}
