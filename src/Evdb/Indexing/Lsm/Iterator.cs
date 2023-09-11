using System.Diagnostics;

namespace Evdb.Indexing.Lsm;

internal interface IIterator
{
    ReadOnlySpan<byte> Key { get; }
    ReadOnlySpan<byte> Value { get; }

    bool Valid();

    void MoveToMin();
    void MoveTo(ReadOnlySpan<byte> key);
    void MoveNext();
}

internal class MergeIterator : IIterator
{
    private IIterator _curr;
    private readonly IIterator[] _all;

    public ReadOnlySpan<byte> Key => _curr.Key;
    public ReadOnlySpan<byte> Value => _curr.Value;

    public MergeIterator(IIterator[] iters)
    {
        Debug.Assert(iters.Length > 0);

        _all = iters;
        _curr = GetMinIterator();
    }

    public bool Valid()
    {
        return _curr.Valid();
    }

    public void MoveToMin()
    {
        foreach (IIterator iter in _all)
        {
            iter.MoveToMin();
        }

        _curr = GetMinIterator();
    }

    public void MoveTo(ReadOnlySpan<byte> key)
    {
        foreach (IIterator iter in _all)
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
        IIterator minIter = _all[0];

        for (int i = 1; i < _all.Length; i++)
        {
            if (_all[i].Key.SequenceCompareTo(minIter.Key) < 0)
            {
                minIter = _all[i];
            }
        }

        return minIter;
    }
}
