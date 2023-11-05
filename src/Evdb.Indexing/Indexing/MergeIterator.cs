using System.Diagnostics;

namespace Evdb.Indexing;

internal sealed class MergeIterator : IIterator
{
    private bool _disposed;
    private IIterator? _curr;
    private readonly IIterator[] _iters;

    public ReadOnlySpan<byte> Key => _curr!.Key;
    public ReadOnlySpan<byte> Value => _curr!.Value;
    public bool IsValid => _curr != null && _curr.IsValid;

    public MergeIterator(IIterator[] iters)
    {
        Debug.Assert(iters.Length > 0);

        _iters = iters;
        _curr = null;
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
        _curr?.MoveNext();
        _curr = GetMinIterator();
    }

    private IIterator? GetMinIterator()
    {
        IIterator minIter = _iters[0];

        for (int i = 1; i < _iters.Length; i++)
        {
            IIterator iter = _iters[i];

            if (iter.IsValid && iter.Key.SequenceCompareTo(minIter.Key) < 0)
            {
                minIter = iter;
            }
        }

        return minIter.IsValid ? minIter : null;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _curr = null;

        foreach (IIterator iter in _iters)
        {
            iter.Dispose();
        }

        _disposed = true;
    }
}
