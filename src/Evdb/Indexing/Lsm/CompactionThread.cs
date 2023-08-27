namespace Evdb.Indexing.Lsm;

internal sealed class CompactionThread : IDisposable
{
    private static int s_nextId = 0;

    private bool _disposed;
    private readonly Thread _thread;
    private readonly CompactionQueue _queue;

    public CompactionThread(CompactionQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue, nameof(queue));

        _queue = queue;
        _thread = new Thread(Work)
        {
            Name = $"Evdb.Lsm.CompactionThread #{Interlocked.Increment(ref s_nextId)}"
        };
        _thread.Start();
    }

    private void Work()
    {
        while (!_disposed && _queue.TryDequeue(out CompactionJob job))
        {
            job.Callback(job.Table);
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _thread.Join();
    }
}
