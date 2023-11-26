namespace LogsDb.Formats;

internal interface IPhysicalTableReader : IDisposable
{
    Status Open();
    Status Get(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
    IIterator GetIterator();
}
