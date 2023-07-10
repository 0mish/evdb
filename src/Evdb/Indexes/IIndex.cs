namespace Evdb.Indexes;

// TODO: Change key type from string to ReadOnlySpan<byte>.
public interface IIndex : IDisposable
{
    bool TrySet(string key, in ReadOnlySpan<byte> value);
    bool TryGet(string key, out ReadOnlySpan<byte> value);
}