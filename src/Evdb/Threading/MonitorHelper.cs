namespace Evdb.Threading;

internal static class MonitorHelper
{
    public static void Exit(object obj, out bool lockReleased)
    {
        if (!Monitor.IsEntered(obj))
        {
            lockReleased = false;

            return;
        }

        Monitor.Exit(obj);

        lockReleased = true;
    }
}
