namespace LogsDb;

internal readonly struct CompactionJob
{
    public VirtualTable Table { get; }
    public Action<VirtualTable> Callback { get; }

    public CompactionJob(VirtualTable table, Action<VirtualTable> callback)
    {
        Table = table;
        Callback = callback;
    }
}
