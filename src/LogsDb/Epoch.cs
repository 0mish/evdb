using System.Diagnostics;

namespace LogsDb;

internal static class Epoch
{
    private const int ActionsSize = 16;
    private const int ThreadsSize = 128;

    private static ulong GlobalEpoch;

    private static int _actionsCount;
    private static readonly EpochAction[] _actions;
    private static readonly EpochThread[] _threads;

    public static bool IsProtected => EpochThread.Index != EpochThread.UnassignedIndex;

    static Epoch()
    {
        _actionsCount = 0;
        _actions = new EpochAction[ActionsSize];
        _threads = new EpochThread[ThreadsSize];

        for (int i = 0; i < _actions.Length; i++)
        {
            _actions[i].Epoch = ulong.MaxValue;
        }

        for (int i = 0; i < _threads.Length; i++)
        {
            _threads[i].Epoch = ulong.MaxValue;
        }
    }

    public static void Acquire()
    {
        Debug.Assert(!IsProtected, "Current thread is already epoch protected.");

        while (true)
        {
            bool set = false;

            for (int i = EpochThread.UnassignedIndex + 1; i < _threads.Length; i++)
            {
                ref EpochThread ethread = ref _threads[i];

                if (Interlocked.CompareExchange(ref ethread.Epoch, GlobalEpoch, ulong.MaxValue) == ulong.MaxValue)
                {
                    EpochThread.Index = i;

                    set = true;

                    break;
                }
            }

            if (set)
            {
                break;
            }

            Thread.Yield();
        }
    }

    public static void Release()
    {
        Debug.Assert(IsProtected, "Current thread is not epoch protected.");

        if (_actionsCount > 0)
        {
            Drain();
        }

        _threads[EpochThread.Index].Epoch = ulong.MaxValue;

        EpochThread.Index = EpochThread.UnassignedIndex;
    }

    public static void Defer(Action action)
    {
        Debug.Assert(IsProtected, "Current thread is not epoch protected.");

        while (true)
        {
            bool set = false;

            for (int i = 0; i < _actions.Length; i++)
            {
                ref EpochAction eaction = ref _actions[i];

                if (Interlocked.CompareExchange(ref eaction.Epoch, ulong.MaxValue - 1, ulong.MaxValue) == ulong.MaxValue)
                {
                    eaction.Action = action;
                    eaction.Epoch = Interlocked.Increment(ref GlobalEpoch) - 1;

                    Interlocked.Increment(ref _actionsCount);

                    set = true;

                    break;
                }
            }

            if (set)
            {
                break;
            }

            if (_actionsCount > 0)
            {
                Drain();
            }

            Thread.Yield();
        }
    }

    private static void Drain()
    {
        ulong safeEpoch = ulong.MaxValue;

        for (int i = 0; i < _threads.Length; i++)
        {
            safeEpoch = ulong.Min(safeEpoch, _threads[i].Epoch);
        }

        for (int i = 0; i < _actions.Length && _actionsCount > 0; i++)
        {
            ref EpochAction eaction = ref _actions[i];
            ulong epoch = eaction.Epoch;

            if (eaction.Epoch < safeEpoch && Interlocked.CompareExchange(ref eaction.Epoch, ulong.MaxValue - 1, epoch) == epoch)
            {
                eaction.Action();
                eaction.Action = null!;
                eaction.Epoch = ulong.MaxValue;

                Interlocked.Decrement(ref _actionsCount);
            }
        }
    }

    private struct EpochAction
    {
        public ulong Epoch;
        public Action Action;

        public EpochAction(ulong epoch, Action action)
        {
            Epoch = epoch;
            Action = action;
        }
    }

    private struct EpochThread
    {
        public const int UnassignedIndex = 0;

        [ThreadStatic]
        public static int Index;

        public ulong Epoch;
    }
}
