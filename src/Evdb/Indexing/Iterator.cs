namespace Evdb.Indexing;

internal interface IIterator : IDisposable
{
    ReadOnlySpan<byte> Key { get; }
    ReadOnlySpan<byte> Value { get; }

    bool Valid();

    void MoveToFirst();
    void MoveTo(ReadOnlySpan<byte> key);
    void MoveNext();
}
