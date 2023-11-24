using System.Diagnostics;

namespace Evdb.Storage.LogsDb;

[DebuggerDisplay("Count = {Count}")]
internal sealed class CompactionQueue : IDisposable
{
    private long _disposed;
    private readonly object _sync;
    private readonly Queue<CompactionJob> _queue;
    private readonly ManualResetEventSlim _notEmpty;

    public int Count => _queue.Count;

    public CompactionQueue()
    {
        _sync = new object();
        _queue = new Queue<CompactionJob>();
        _notEmpty = new ManualResetEventSlim(initialState: false);
    }

    public void Enqueue(in CompactionJob job)
    {
        if (Interlocked.Read(ref _disposed) == 1)
        {
            return;
        }

        lock (_sync)
        {
            _queue.Enqueue(job);
            _notEmpty.Set();
        }
    }

    public bool TryDequeue(out CompactionJob job)
    {
        while (Interlocked.Read(ref _disposed) != 1 || _notEmpty.IsSet)
        {
            _notEmpty.Wait();

            lock (_sync)
            {
                if (_queue.Count == 0)
                {
                    _notEmpty.Reset();

                    continue;
                }

                job = _queue.Dequeue();
            }

            return true;
        }

        job = default;

        return false;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        // If the not empty event is set, wait until it is reset, i.e. we wait until the queue is empty.
        while (_notEmpty.IsSet)
        {
            Thread.Yield();
        }

        lock (_sync)
        {
            _notEmpty.Set();
        }
    }
}
