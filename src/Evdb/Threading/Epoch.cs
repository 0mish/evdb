namespace Evdb.Threading;

internal sealed class Epoch
{
    [ThreadStatic]
    private static ulong LocalEpoch;
    private static ulong GlobalEpoch;

    private static Dictionary<int, ulong> Epochs { get; }
    private static List<EpochAction> Actions { get; }

    static Epoch()
    {
        Epochs = new Dictionary<int, ulong>();
        Actions = new List<EpochAction>();
    }

    public static void Acquire()
    {
        LocalEpoch = GlobalEpoch;

        Epochs[Environment.CurrentManagedThreadId] = LocalEpoch;
    }

    public static void Release()
    {
        lock (Actions)
        {
            foreach (EpochAction action in Actions)
            {
                if (GlobalEpoch > action.Epoch)
                {
                    action.Action();
                }
            }
        }
    }

    public static void Retire(Action action)
    {
        lock (Actions)
        {
            Actions.Add(new EpochAction(LocalEpoch, action));
        }
    }

    private readonly struct EpochAction
    {
        public ulong Epoch { get; }
        public Action Action { get; }

        public EpochAction(ulong epoch, Action action) =>
            (Epoch, Action) = (epoch, action);
    }
}
