namespace Evdb.Indexing;

internal readonly struct ManifestEdit
{
    public ListEdit<VirtualTable> VirtualTables { get; }
    public ListEdit<PhysicalTable> PhysicalTables { get; }
    public ListEdit<PhysicalLog> PhysicalLogs { get; }

    public ManifestEdit(ListEdit<VirtualTable> vtables = default, ListEdit<PhysicalTable> ptables = default, ListEdit<PhysicalLog> plogs = default)
    {
        VirtualTables = vtables;
        PhysicalTables = ptables;
        PhysicalLogs = plogs;
    }
}

internal readonly struct ListEdit<T>
{
    public T[]? Registered { get; }
    public T[]? Unregistered { get; }

    public ListEdit(T[]? registered = null, T[]? unregistered = null)
    {
        Registered = registered;
        Unregistered = unregistered;
    }
}
