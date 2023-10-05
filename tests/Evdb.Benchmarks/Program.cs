using Evdb.Indexing.Lsm;
using System.Diagnostics;

DatabaseOptions options = new()
{
    Path = "db",

    // Purposefully use a small virtual table size to force compaction and disk reads.
    VirtualTableSize = 1024 * 16
};

Func<BenchmarkResult>[] benchmarks = new Func<BenchmarkResult>[]
{
    () => SingleWriterSingleReader(entries: 10000),
    () => MultipleWritersSingleReader(entries: 10000),
    () => SingleWriterMultipleReader(entries: 10000),
    () => ConcurrnetMultipleWritersMultipleReader(entries: 10000),
};

string header = $"{"Benchmark Name",50} | {"Bytes Written/s",18:f2} | {"Bytes Read/s",18:f2} | {"Misses",10}";
string divider = new('-', header.Length);

Console.WriteLine(header);
Console.WriteLine(divider);

foreach (Func<BenchmarkResult> benchmark in benchmarks)
{
    const int WarmUpCount = 5;

    // Warm up before actual result.
    for (int i = 0; i < WarmUpCount; i++)
    {
        benchmark();
    }

    BenchmarkResult result = benchmark();

    const double Scale = 1024 * 1024;

    double bytesWrittenPerSecond = result.BytesWritten / Scale / result.WriteDuration.TotalSeconds;
    double bytesReadPerSecond = result.BytesRead / Scale / result.ReadDuration.TotalSeconds;

    Console.WriteLine($"{result.Name,50} | {bytesWrittenPerSecond,13:f2} mb/s | {bytesReadPerSecond,13:f2} mb/s | {result.Misses,10}");
}

BenchmarkResult ConcurrnetMultipleWritersMultipleReader(int entries)
{
    List<KeyValuePair<byte[], byte[]>> kvs = GenerateKeyValues(entries, keySize: 12, valueSize: 64);

    using Database db = new(options);

    TimeSpan wts = default;

    // TODO: Use a fixed number of threads here.
    Task wtask = Task.Run(() =>
    {
        Stopwatch wsw = Stopwatch.StartNew();

        Parallel.ForEach(kvs, kv =>
        {
            db.TrySet(kv.Key, kv.Value);
        });

        wts = wsw.Elapsed;
    });

    ulong miss = 0;
    TimeSpan rts = default;

    // TODO: Use a fixed number of threads here.
    Task rtask = Task.Run(() =>
    {
        Stopwatch rsw = Stopwatch.StartNew();

        Parallel.ForEach(kvs, kv =>
        {
            if (!db.TryGet(kv.Key, out ReadOnlySpan<byte> val) || !val.SequenceEqual(kv.Value))
            {
                miss++;
            }
        });

        rts = rsw.Elapsed;
    });

    Task.WaitAll(wtask, rtask);

    // TODO: Actually calculate metric from the index.
    ulong readWrite = (ulong)entries * (12 + 64);

    return new BenchmarkResult("Multiple Writers and Multiple Readers", readWrite, readWrite, miss, wts, rts);
}

BenchmarkResult MultipleWritersSingleReader(int entries)
{
    List<KeyValuePair<byte[], byte[]>> kvs = GenerateKeyValues(entries, keySize: 12, valueSize: 64);

    using Database db = new(options);

    Stopwatch sw = Stopwatch.StartNew();

    // TODO: Use a fixed number of threads here.
    Parallel.ForEach(kvs, kv =>
    {
        db.TrySet(kv.Key, kv.Value);
    });

    ulong miss = 0;
    TimeSpan wts = sw.Elapsed;

    foreach (KeyValuePair<byte[], byte[]> kv in kvs)
    {
        if (!db.TryGet(kv.Key, out ReadOnlySpan<byte> val) || !val.SequenceEqual(kv.Value))
        {
            miss++;
        }
    }

    // TODO: Actually calculate metric from the index.
    TimeSpan rts = sw.Elapsed - wts;
    ulong readWrite = (ulong)entries * (12 + 64);

    return new BenchmarkResult("Multiple Writers then Single Reader", readWrite, readWrite, miss, wts, rts);
}

BenchmarkResult SingleWriterSingleReader(int entries)
{
    List<KeyValuePair<byte[], byte[]>> kvs = GenerateKeyValues(entries, keySize: 12, valueSize: 64);

    using Database db = new(options);

    Stopwatch sw = Stopwatch.StartNew();

    foreach (KeyValuePair<byte[], byte[]> kv in kvs)
    {
        db.TrySet(kv.Key, kv.Value);
    }

    ulong miss = 0;
    TimeSpan wts = sw.Elapsed;

    foreach (KeyValuePair<byte[], byte[]> kv in kvs)
    {
        if (!db.TryGet(kv.Key, out ReadOnlySpan<byte> val) || !val.SequenceEqual(kv.Value))
        {
            miss++;
        }
    }

    // TODO: Actually calculate metric from the index.
    TimeSpan rts = sw.Elapsed - wts;
    ulong readWrite = (ulong)entries * (12 + 64);

    return new BenchmarkResult("Single Writer then Single Reader", readWrite, readWrite, miss, wts, rts);
}

BenchmarkResult SingleWriterMultipleReader(int entries)
{
    List<KeyValuePair<byte[], byte[]>> kvs = GenerateKeyValues(entries, keySize: 12, valueSize: 64);

    using Database db = new(options);

    Stopwatch sw = Stopwatch.StartNew();

    foreach (KeyValuePair<byte[], byte[]> kv in kvs)
    {
        db.TrySet(kv.Key, kv.Value);
    }

    ulong miss = 0;
    TimeSpan wts = sw.Elapsed;

    // TODO: Use a fixed number of threads here.
    Parallel.ForEach(kvs, kv =>
    {
        if (!db.TryGet(kv.Key, out ReadOnlySpan<byte> val) || !val.SequenceEqual(kv.Value))
        {
            miss++;
        }
    });

    // TODO: Actually calculate metric from the index.
    TimeSpan rts = sw.Elapsed - wts;
    ulong readWrite = (ulong)entries * (12 + 64);

    return new BenchmarkResult("Single Writer then Multiple Readers", readWrite, readWrite, miss, wts, rts);
}

static List<KeyValuePair<byte[], byte[]>> GenerateKeyValues(int count, int keySize, int valueSize)
{
    Random random = new(Seed: 0);
    List<KeyValuePair<byte[], byte[]>> kvs = new();

    byte[] key = new byte[keySize];
    byte[] value = new byte[valueSize];

    for (int i = 0; i < count; i++)
    {
        random.NextBytes(key);
        random.NextBytes(value);

        kvs.Add(new(key.ToArray(), value.ToArray()));
    }

    return kvs;
}

record class BenchmarkResult(string Name, ulong BytesWritten, ulong BytesRead, ulong Misses, TimeSpan WriteDuration, TimeSpan ReadDuration);