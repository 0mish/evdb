namespace Evdb.Indexing;

internal interface IIndex
{
    bool TrySet(string key, in ReadOnlySpan<byte> value);
    bool TryGet(string key, out ReadOnlySpan<byte> value);
}
