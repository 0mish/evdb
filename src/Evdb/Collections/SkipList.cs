namespace Evdb.Collections;

internal sealed class SkipList
{
    private const int MaxHeight = 13;

    private int _height;
    private Node _head;
    private readonly Random _rand;

    public int Count { get; private set; }

    public SkipList()
    {
        _height = 0;
        _head = new Node(default!, default!, MaxHeight);
        _rand = new Random(0);
    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Node[] prevs = new Node[MaxHeight];

        int height = RandomHeight();

        FindGreaterOrEqual(key, prevs);

        if (height > _height)
        {
            for (int i = _height; i < height; i++)
            {
                prevs[i] = _head;
            }

            _height = height;
        }

        Node node = new(key.ToArray(), value.ToArray(), height);

        for (int i = 0; i < height; i++)
        {
            node.Next[i] = prevs[i].Next[i];
            prevs[i].Next[i] = node;
        }

        Count++;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        Node? node = FindGreaterOrEqual(key);

        // TODO: optimize - Avoid a second sequence compare.
        if (node != null && key.SequenceCompareTo(node.Key) == 0)
        {
            value = node.Value;

            return true;
        }

        value = default;

        return false;
    }

    public bool TryGetMax(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        Node? node = FindMax();

        if (node != null)
        {
            key = node.Key;
            value = node.Value;

            return false;
        }

        key = default;
        value = default;

        return false;
    }

    public bool TryGetMin(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        Node? node = FindMin();

        if (node != null)
        {
            key = node.Key;
            value = node.Value;

            return false;
        }

        key = default;
        value = default;

        return false;
    }

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    private Node? FindMin()
    {
        return _head.Next[0];
    }

    private Node? FindMax()
    {
        Node? node = _head;

        for (int i = _height; i >= 0; i--)
        {
            Node? next = node.Next[i];

            while (next != null)
            {
                node = next;
                next = next.Next[i];
            }
        }

        return node;
    }

    private Node? FindLess(ReadOnlySpan<byte> key)
    {
        Node node = _head;

        for (int i = _height; i >= 0; i--)
        {
            Node? next = node.Next[i];

            while (next != null && key.SequenceCompareTo(next.Key) > 0)
            {
                node = next;
                next = next.Next[i];
            }
        }

        return node;
    }

    private Node? FindGreaterOrEqual(ReadOnlySpan<byte> key, Node?[]? prevs = null)
    {
        Node node = _head;
        Node? next = null;

        for (int i = _height - 1; i >= 0; i--)
        {
            next = node.Next[i];

            while (next != null && key.SequenceCompareTo(next.Key) > 0)
            {
                node = next;
                next = next.Next[i];
            }

            if (prevs != null)
            {
                prevs[i] = node;
            }
        }

        return next;
    }

    private int RandomHeight()
    {
        int result = 1;

        while (result < MaxHeight && _rand.Next() % 2 == 0)
        {
            result++;
        }

        return result;
    }

    internal class Node
    {
        private readonly byte[] _key;
        private readonly byte[] _value;

        public ReadOnlySpan<byte> Key => _key;
        public ReadOnlySpan<byte> Value => _value;
        public Node[] Next { get; }

        public Node(byte[] key, byte[] value, int height)
        {
            _key = key;
            _value = value;
            Next = new Node[height + 1];
        }
    }

    public struct Iterator
    {
        private Node? _node;
        private readonly SkipList _sl;

        internal Iterator(SkipList sl)
        {
            _sl = sl;

            MoveToMin();
        }

        public void MoveToMin()
        {
            _node = _sl.FindMin();
        }

        public void MoveToMax()
        {
            _node = _sl.FindMax();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _node = _sl.FindGreaterOrEqual(key);
        }

        public bool TryMoveNext(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            if (_node != null)
            {
                key = _node.Key;
                value = _node.Value;

                _node = _node.Next[0];

                return true;
            }

            key = default;
            value = default;

            return false;
        }

        public bool TryMovePrevious(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            if (_node != null)
            {
                Node? node = _sl.FindLess(_node.Key);

                if (node != null)
                {
                    key = node.Key;
                    value = node.Value;

                    return true;
                }
            }

            key = default;
            value = default;

            return false;
        }
    }
}
