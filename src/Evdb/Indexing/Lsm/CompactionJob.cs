namespace Evdb.Indexing.Lsm;

internal readonly struct CompactionJob
{
    public VirtualTable Table { get; }
    public Action<VirtualTable> Callback { get; }

    public CompactionJob(VirtualTable table, Action<VirtualTable> callback)
    {
        ArgumentNullException.ThrowIfNull(table, nameof(table));
        ArgumentNullException.ThrowIfNull(callback, nameof(callback));

        Table = table;
        Callback = callback;
    }
}
