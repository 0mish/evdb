using System.Diagnostics;

namespace Evdb.Benchmarks.Diagnostics;

public struct TimeMeasurement
{
    private long _start;
    private long _end;

    public readonly TimeSpan Duration => Stopwatch.GetElapsedTime(_start, _end);

    public TimeMeasurement()
    {
        _start = long.MaxValue;
        _end = long.MinValue;
    }

    public void Begin()
    {
        long start = Stopwatch.GetTimestamp();
        long currStart = _start;

        while (start < currStart)
        {
            currStart = Interlocked.CompareExchange(ref _start, start, currStart);
        }
    }

    public void End()
    {
        long end = Stopwatch.GetTimestamp();
        long currEnd = _end;

        while (end > currEnd)
        {
            currEnd = Interlocked.CompareExchange(ref _end, end, currEnd);
        }
    }
}
