namespace LogsDb;

internal sealed class ManifestState
{
    public VirtualTable[] VirtualTables { get; }
    public PhysicalTable[] PhysicalTables { get; }
    public PhysicalLog[] PhysicalLogs { get; }

    public ManifestState(VirtualTable[] vtables, PhysicalTable[] ptables, PhysicalLog[] plogs)
    {
        VirtualTables = vtables;
        PhysicalTables = ptables;
        PhysicalLogs = plogs;
    }
}
