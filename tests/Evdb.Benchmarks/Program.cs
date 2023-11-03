using Evdb.Benchmarks;
using Evdb.Benchmarks.Drivers;

RunSuite<EvdbBenchmarkDriver>();
RunSuite<RocksDbBenchmarkDriver>();

static void RunSuite<TDriver>() where TDriver : IBenchmarkDriver
{
    BenchmarkOptions[] benchmarks = new BenchmarkOptions[]
    {
        new BenchmarkOptions
        {
            Entries = 100000,
            WriterThreads = 1,
            ReaderThreads = 1
        },
        new BenchmarkOptions
        {
            Entries = 100000,
            WriterThreads = Environment.ProcessorCount,
            ReaderThreads = 1
        },
        new BenchmarkOptions
        {
            Entries = 100000,
            WriterThreads = 1,
            ReaderThreads = Environment.ProcessorCount
        }
    };

    PrintHeader();

    foreach (BenchmarkOptions options in benchmarks)
    {
        BenchmarkResult result = Run<TDriver>(options);

        PrintResult(result);
    }

    Console.WriteLine();
}

static BenchmarkResult Run<TDriver>(BenchmarkOptions options) where TDriver : IBenchmarkDriver
{
    if (typeof(TDriver) == typeof(EvdbBenchmarkDriver))
    {
        EvdbBenchmarkDriverOptions driverOptions = new()
        {
            VirtualTableSize = 1024 * 1024 * 16
        };

        EvdbBenchmarkDriver driver = new(driverOptions);

        using Benchmark<EvdbBenchmarkDriver> bench = new(options, driver);

        BenchmarkResult result = bench.Run();

        return result;
    }
    else if (typeof(TDriver) == typeof(RocksDbBenchmarkDriver))
    {
        RocksDbBenchmarkDriverOptions driverOptions = new()
        {
            WriteBufferSize = 1024 * 1024 * 16,

            // Disable auto compaction for a more apples to apples comparison with evdb.
            //
            // TODO: Turn this back on when evdb supports table compaction.
            AutoCompaction = false,

            // Disable paranoid checks since evdb does not perform any checks.
            //
            // TODO: Turn this back on when evdb supports redundancy checks.
            ParanoidChecks = false
        };

        RocksDbBenchmarkDriver driver = new(driverOptions);

        using Benchmark<RocksDbBenchmarkDriver> bench = new(options, driver);

        BenchmarkResult result = bench.Run();

        return result;
    }

    throw new NotSupportedException();
}

static void PrintHeader()
{
    string header = $"{"Benchmark Name",50} | {"Bytes Written/s",18:f2} | {"Bytes Read/s",18:f2} | {"Misses",10}";
    string divider = new('-', header.Length);

    Console.WriteLine(header);
    Console.WriteLine(divider);
}

static void PrintResult(BenchmarkResult result)
{
    const double Scale = 1024 * 1024;

    double bytesWrittenPerSecond = result.BytesWritten / Scale / result.WriteDuration.TotalSeconds;
    double bytesReadPerSecond = result.BytesRead / Scale / result.ReadDuration.TotalSeconds;

    Console.WriteLine($"{result.Name,50} | {bytesWrittenPerSecond,13:f2} mb/s | {bytesReadPerSecond,13:f2} mb/s | {result.Misses,10}");
}
