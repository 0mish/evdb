namespace Evdb.Storage;

public interface IDatabase : IDisposable
{
    Status Open();
    Status Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    Status Get(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
}
