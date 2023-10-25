using Evdb.Benchmarks.Diagnostics;

namespace Evdb.Benchmarks;

public class Benchmark<TDriver> : IDisposable where TDriver : IBenchmarkDriver
{
    private bool _disposed;
    private ulong _misses;

    private TimeMeasurement _writeTime;
    private TimeMeasurement _readTime;

    private readonly CountdownEvent _writeCounter;
    private readonly CountdownEvent _readCounter;
    private readonly ManualResetEventSlim _writeEvent;
    private readonly ManualResetEventSlim _readEvent;

    private readonly BenchmarkOptions _options;
    private readonly TDriver _driver;
    private readonly List<KeyValuePair<byte[], byte[]>> _kvs;

    private readonly Thread[] _writers;
    private readonly Thread[] _readers;

    public Benchmark(BenchmarkOptions options, TDriver driver)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _driver = driver;

        _kvs = GenerateKeyValues(options.Entries, options.KeyLength, options.ValueLength);

        _writeTime = new TimeMeasurement();
        _readTime = new TimeMeasurement();

        _writeEvent = new ManualResetEventSlim(initialState: false);
        _readEvent = new ManualResetEventSlim(initialState: false);

        _writers = new Thread[options.WriterThreads];
        _readers = new Thread[options.ReaderThreads];

        _writeCounter = new CountdownEvent(initialCount: _writers.Length);
        _readCounter = new CountdownEvent(initialCount: _readers.Length);

        for (int i = 0; i < _writers.Length; i++)
        {
            _writers[i] = new Thread(WriteWork);
            _writers[i].Start(i);
        }

        for (int i = 0; i < _readers.Length; i++)
        {
            _readers[i] = new Thread(ReadWork);
            _readers[i].Start(i);
        }
    }

    public BenchmarkResult Run()
    {
        _writeEvent.Set();
        _writeCounter.Wait();

        _driver.WaitCompaction();

        _readEvent.Set();
        _readCounter.Wait();

        TimeSpan wts = _writeTime.Duration;
        TimeSpan rts = _readTime.Duration;

        ulong readWrite = (ulong)_options.Entries * (ulong)(_options.KeyLength + _options.ValueLength);

        return new BenchmarkResult($"{_writers.Length} Writers then {_readers.Length} Readers", readWrite, readWrite, _misses, wts, rts);
    }

    private void WriteWork(object? param)
    {
        if (param is not int index)
        {
            return;
        }

        _writeEvent.Wait();
        _writeTime.Begin();

        // FIXME: Figure out the remainders.
        int chunk = _kvs.Count / _options.WriterThreads;

        for (int i = index * chunk; i < (index + 1) * chunk; i++)
        {
            KeyValuePair<byte[], byte[]> kv = _kvs[i];

            _driver.TrySet(kv.Key, kv.Value);
        }

        _writeTime.End();
        _writeCounter.Signal();
    }

    private void ReadWork(object? param)
    {
        if (param is not int index)
        {
            return;
        }

        _readEvent.Wait();
        _readTime.Begin();

        // FIXME: Figure out the remainders.
        int chunk = _kvs.Count / _options.ReaderThreads;

        for (int i = index * chunk; i < (index + 1) * chunk; i++)
        {
            KeyValuePair<byte[], byte[]> kv = _kvs[i];

            if (!_driver.TryGet(kv.Key, out ReadOnlySpan<byte> value) || !value.SequenceEqual(kv.Value))
            {
                Interlocked.Increment(ref _misses);
            }
        }

        _readTime.End();
        _readCounter.Signal();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);

        if (_driver is IDisposable driver)
        {
            driver.Dispose();
        }

        _disposed = true;
    }

    private static List<KeyValuePair<byte[], byte[]>> GenerateKeyValues(int count, int keySize, int valueSize)
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
}
