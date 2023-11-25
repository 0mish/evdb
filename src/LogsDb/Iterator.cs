namespace LogsDb;

internal interface IIterator : IDisposable
{
    ReadOnlySpan<byte> Key { get; }
    ReadOnlySpan<byte> Value { get; }
    bool IsValid { get; }

    void MoveToFirst();
    void MoveTo(ReadOnlySpan<byte> key);
    void MoveNext();
}
