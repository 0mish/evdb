namespace LogsDb.Formats;

internal interface IPhysicalTableWriter : IDisposable
{
    void Write(VirtualTable vtable);
}
