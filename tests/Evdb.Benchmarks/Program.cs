using Evdb.Indexes.Lsm;
using Evdb.IO;
using System.Diagnostics;
using System.Text;

SingleWriterSingleReader(iter: 10, entries: 1000);
MultipleWritersSingleReader(iter: 10, entries: 1000);

static void MultipleWritersSingleReader(int iter, int entries)
{
    Console.WriteLine($"{nameof(MultipleWritersSingleReader)}:");

    ulong tmiss = 0;
    TimeSpan awts = default;
    TimeSpan arts = default;

    Dictionary<string, byte[]> kvs = GenerateKeyValues(entries);

    for (int j = 0; j < iter; j++)
    {
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

        TimeSpan rts = sw.Elapsed - wts;

        Console.WriteLine($"Done in {(rts + wts).TotalMilliseconds}ms #{j}");
        Console.WriteLine($" w {wts.TotalMilliseconds} ms {entries / wts.TotalMilliseconds:f2} p/s");
        Console.WriteLine($" r {rts.TotalMilliseconds} ms {entries / rts.TotalMilliseconds:f2} g/s {miss} misses");

        awts += wts;
        arts += rts;
        tmiss += miss;
    }

    Console.WriteLine();
    Console.WriteLine($"Done in {(arts + awts).TotalMilliseconds}ms <>");
    Console.WriteLine($" w {awts.TotalMilliseconds} ms {iter * entries / awts.TotalMilliseconds:f2} p/s");
    Console.WriteLine($" r {arts.TotalMilliseconds} ms {iter * entries / arts.TotalMilliseconds:f2} g/s {tmiss} misses");
    Console.WriteLine();
    Console.WriteLine();
}

static void SingleWriterSingleReader(int iter, int entries)
{
    Console.WriteLine($"{nameof(SingleWriterSingleReader)}:");

    ulong tmiss = 0;
    TimeSpan awts = default;
    TimeSpan arts = default;

    Dictionary<string, byte[]> kvs = GenerateKeyValues(entries);

    for (int j = 0; j < iter; j++)
    {
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

        TimeSpan rts = sw.Elapsed - wts;

        Console.WriteLine($"Done in {(rts + wts).TotalMilliseconds}ms #{j}");
        Console.WriteLine($" w {wts.TotalMilliseconds} ms {entries / wts.TotalMilliseconds:f2} p/s");
        Console.WriteLine($" r {rts.TotalMilliseconds} ms {entries / rts.TotalMilliseconds:f2} g/s {miss} misses");

        awts += wts;
        arts += rts;
        tmiss += miss;
    }

    Console.WriteLine();
    Console.WriteLine($"Done in {(arts + awts).TotalMilliseconds}ms <>");
    Console.WriteLine($" w {awts.TotalMilliseconds} ms {iter * entries / awts.TotalMilliseconds:f2} p/s");
    Console.WriteLine($" r {arts.TotalMilliseconds} ms {iter * entries / arts.TotalMilliseconds:f2} g/s {tmiss} misses");
    Console.WriteLine();
    Console.WriteLine();
}

static Dictionary<string, byte[]> GenerateKeyValues(int count)
{
    Random random = new(Seed: 0);
    Dictionary<string, byte[]> kvs = new();

    byte[] key = new byte[12];
    byte[] value = new byte[64];

    for (int i = 0; i < count; i++)
    {
        random.NextBytes(key);
        random.NextBytes(value);

        kvs[Encoding.Unicode.GetString(key)] = value.ToArray();
    }

    return kvs;
}
