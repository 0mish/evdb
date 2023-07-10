namespace Evdb.Indexes.Lsm;

public readonly struct CompactionJob
{
    public VirtualTable Table { get; }
    public Action<VirtualTable, PhysicalTable> Callback { get; }

    public CompactionJob(VirtualTable table, Action<VirtualTable, PhysicalTable> callback)
    {
        ArgumentNullException.ThrowIfNull(table, nameof(table));
        ArgumentNullException.ThrowIfNull(callback, nameof(callback));

        Table = table;
        Callback = callback;
    }
}
