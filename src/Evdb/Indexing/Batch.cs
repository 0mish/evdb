namespace Evdb.Indexing;

internal sealed class Batch
{
    public bool Applied { get; private set; }

    public void Set(string key, in ReadOnlySpan<byte> data)
    {
        throw new NotImplementedException();
    }
}
