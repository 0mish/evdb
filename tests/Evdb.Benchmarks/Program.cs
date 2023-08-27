using Evdb.Indexes.Lsm;
using Evdb.IO;
using System.Diagnostics;
using System.Text;

Func<BenchmarkResult>[] benchmarks = new Func<BenchmarkResult>[]
{
    () => SingleWriterSingleReader(entries: 10000),
    () => MultipleWritersSingleReader(entries: 10000)
};

string header = $"{"Benchmark Name",50} | {"Bytes Written/s",18:f2} | {"Bytes Read/s",18:f2}";
string divider = new('-', header.Length);

Console.WriteLine(header);
Console.WriteLine(divider);

foreach (Func<BenchmarkResult> benchmark in benchmarks)
{
    BenchmarkResult result = benchmark();

    double bytesWrittenPerSecond = result.BytesWritten / result.Duration.TotalSeconds;
    double bytesReadPerSecond = result.BytesRead / result.Duration.TotalSeconds;

    Console.WriteLine($"{result.Name,50} | {bytesWrittenPerSecond,10:f2} bytes/s | {bytesReadPerSecond,10:f2} bytes/s");
}

static BenchmarkResult MultipleWritersSingleReader(int entries)
{
    Dictionary<string, byte[]> kvs = GenerateKeyValues(entries, keySize: 12, valueSize: 64);
    LsmIndexOptions options = new()
    {
        Path = "db"
    };

    using LsmIndex db = new(options);

    Stopwatch sw = Stopwatch.StartNew();

    // TODO: Use a fixed number of threads here.
    Parallel.ForEach(kvs, kv =>
    {
        db.TrySet(kv.Key, kv.Value);
    });

    ulong miss = 0;
    TimeSpan wts = sw.Elapsed;

    foreach (var kv in kvs)
    {
        if (!db.TryGet(kv.Key, out ReadOnlySpan<byte> val) || !val.SequenceEqual(kv.Value))
        {
            miss++;
        }
    }

    // TODO: Actually calculate metric from the index.
    TimeSpan rts = sw.Elapsed - wts;
    ulong readWrite = (ulong)entries * (12 + 64);

    return new BenchmarkResult("Multiple Writers then Single Reader", readWrite, readWrite, wts + rts);
}

static BenchmarkResult SingleWriterSingleReader(int entries)
{
    Dictionary<string, byte[]> kvs = GenerateKeyValues(entries, keySize: 12, valueSize: 64);
    LsmIndexOptions options = new()
    {
        Path = "db",
        FileSystem = new FileSystem()
    };

    using LsmIndex db = new(options);

    Stopwatch sw = Stopwatch.StartNew();

    foreach (var kv in kvs)
    {
        db.TrySet(kv.Key, kv.Value);
    }

    ulong miss = 0;
    TimeSpan wts = sw.Elapsed;

    foreach (var kv in kvs)
    {
        if (!db.TryGet(kv.Key, out ReadOnlySpan<byte> val) || !val.SequenceEqual(kv.Value))
        {
            miss++;
        }
    }

    // TODO: Actually calculate metric from the index.
    TimeSpan rts = sw.Elapsed - wts;
    ulong readWrite = (ulong)entries * (12 + 64);

    return new BenchmarkResult("Single Writer then Single Reader", readWrite, readWrite, wts + rts);
}

static Dictionary<string, byte[]> GenerateKeyValues(int count, int keySize, int valueSize)
{
    Random random = new(Seed: 0);
    Dictionary<string, byte[]> kvs = new();

    byte[] key = new byte[keySize];
    byte[] value = new byte[valueSize];

    for (int i = 0; i < count; i++)
    {
        random.NextBytes(key);
        random.NextBytes(value);

        kvs[Encoding.Unicode.GetString(key)] = value.ToArray();
    }

    return kvs;
}

record class BenchmarkResult(string Name, ulong BytesWritten, ulong BytesRead, TimeSpan Duration);