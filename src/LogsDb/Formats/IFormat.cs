namespace LogsDb.Formats;

internal interface IFormat
{
    IPhysicalTableReader GetTableReader(FileMetadata metadata);
    IPhysicalTableWriter GetTableWriter(VirtualTable table);
}
