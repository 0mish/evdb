using System.Diagnostics;

namespace Evdb.Indexing.Lsm;

internal interface IIterator : IDisposable
{
    ReadOnlySpan<byte> Key { get; }
    ReadOnlySpan<byte> Value { get; }

    bool Valid();

    void MoveToFirst();
    void MoveTo(ReadOnlySpan<byte> key);
    void MoveNext();
}

internal sealed class MergeIterator : IIterator
{
    private bool _disposed;
    private IIterator _curr;
    private readonly IIterator[] _iters;

    public ReadOnlySpan<byte> Key => _curr.Key;
    public ReadOnlySpan<byte> Value => _curr.Value;

    public MergeIterator(IIterator[] iters)
    {
        Debug.Assert(iters.Length > 0);

        _iters = iters;
        _curr = GetMinIterator();
    }

    public bool Valid()
    {
        return _curr.Valid();
    }

    public void MoveToFirst()
    {
        foreach (IIterator iter in _iters)
        {
            iter.MoveToFirst();
        }

        _curr = GetMinIterator();
    }

    public void MoveTo(ReadOnlySpan<byte> key)
    {
        foreach (IIterator iter in _iters)
        {
            iter.MoveTo(key);
        }

        _curr = GetMinIterator();
    }

    public void MoveNext()
    {
        _curr.MoveNext();
        _curr = GetMinIterator();
    }

    private IIterator GetMinIterator()
    {
        IIterator minIter = _iters[0];

        for (int i = 1; i < _iters.Length; i++)
        {
            if (_iters[i].Key.SequenceCompareTo(minIter.Key) < 0)
            {
                minIter = _iters[i];
            }
        }

        return minIter;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        foreach (IIterator iter in _iters)
        {
            iter.Dispose();
        }

        _disposed = true;
    }
}
