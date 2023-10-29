using System.Runtime.CompilerServices;

namespace Evdb.Collections;

internal sealed class SkipList
{
    [ThreadStatic]
    private static (Node?, Node?)[]? s_slice;
    private static readonly int[] s_probs;

    private const int MaxHeight = 13;

    static SkipList()
    {
        // Pre-compute the probability ranges for p = 1/e so that we can use a single random number to determine the
        // tower height.
        //
        // 1/e is the optimal p value to minimize the upper bound for searching in the average case when n is infinite.
        double p = 1.0d;

        s_probs = new int[MaxHeight];

        for (int i = 0; i < MaxHeight; i++)
        {
            s_probs[i] = (int)(int.MaxValue * p);

            p *= 1.0d / Math.E;
        }
    }

    private int _count;
    private int _height;
    private readonly Node _head;

    public int Count => _count;

    public SkipList()
    {
        _height = 0;
        _head = new Node(default!, default!, MaxHeight);
    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        (Node?, Node?)[] slice;

        if (s_slice == null)
        {
            slice = s_slice = new (Node?, Node?)[MaxHeight];
        }
        else
        {
            slice = s_slice;

            Array.Clear(slice);
        }

        Slice(key, slice, out bool found);

        // If the key was already added, this becomes a no op.
        if (found)
        {
            return;
        }

        int height = RandomHeight();
        int currHeight = _height;

        // If height increased we ensure that we do insert a lower one.
        while (currHeight < height && Interlocked.CompareExchange(ref _height, height, currHeight) != currHeight)
        {
            currHeight = _height;
        }

        Node node = new(key.ToArray(), value.ToArray(), height);

        // Insert node starting from lower level to higher level to ensure the invariant that lower level have nodes
        // closer (in terms of key values) to each other.
        for (int level = 0; level < height; level++)
        {
            (Node? prev, Node? next) = slice[level];

            // If `prev` is null, it means `node` increased the height of the skip list therefore the slice is `_head`.
            prev ??= _head;

            while (true)
            {
                node.Next[level] = next;

                // If the previous node was still pointing to the next node we can insert, otherwise it means another
                // thread inserted something between `prev` & `next`.
                if (Interlocked.CompareExchange(ref prev.Next[level], node, next) == next)
                {
                    break;
                }

                // Re-compute new `prev` & `next` for this level.
                (prev, next) = SliceLevel(key, level, prev, out found);

                // If the key got inserted by another thread mean while, this becomes a no op.
                if (found)
                {
                    return;
                }
            }
        }

        Interlocked.Increment(ref _count);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        Node? node = FindGreaterOrEqual(key, out bool found);

        if (found)
        {
            value = node!.Value;

            return true;
        }

        value = default;

        return false;
    }

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    private Node? FindFirst()
    {
        return _head.Next[0];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Node? FindGreaterOrEqual(ReadOnlySpan<byte> key, out bool found)
    {
        found = false;

        Node prev = _head;
        Node? next = null;

        for (int level = _height - 1; level >= 0; level--)
        {
            (prev, next) = SliceLevel(key, level, prev, out found);
        }

        return next;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Slice(ReadOnlySpan<byte> key, (Node?, Node?)[] slice, out bool found)
    {
        found = false;

        Node prev = _head;

        for (int level = _height - 1; level >= 0; level--)
        {
            (prev, Node? next) = SliceLevel(key, level, prev, out found);

            slice[level] = (prev, next);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (Node Previous, Node? Next) SliceLevel(ReadOnlySpan<byte> key, int level, Node start, out bool found)
    {
        found = false;

        Node prev = start;
        Node? next = prev.Next[level];

        while (next != null)
        {
            int cmp = next.Key.SequenceCompareTo(key);

            if (cmp > 0)
            {
                break;
            }
            else if (cmp == 0)
            {
                found = true;
                break;
            }

            prev = next;
            next = next.Next[level];
        }

        return (prev, next);
    }

    private static int RandomHeight()
    {
        int height = 1;
        int value = Random.Shared.Next();

        while (height < MaxHeight && value < s_probs[height])
        {
            height++;
        }

        return height;
    }

    internal class Node
    {
        private readonly byte[] _key;
        private readonly byte[] _value;

        public ReadOnlySpan<byte> Key => _key;
        public ReadOnlySpan<byte> Value => _value;
        public Node?[] Next { get; }

        public Node(byte[] key, byte[] value, int height)
        {
            _key = key;
            _value = value;
            Next = new Node?[height];
        }
    }

    public struct Iterator
    {
        private Node? _node;
        private readonly SkipList _sl;

        public readonly ReadOnlySpan<byte> Key => _node != null ? _node.Key : default;
        public readonly ReadOnlySpan<byte> Value => _node != null ? _node.Value : default;
        public readonly bool IsValid => _node != null;

        public Iterator(SkipList sl)
        {
            _sl = sl;
        }

        public void MoveToFirst()
        {
            _node = _sl.FindFirst();
        }

        public void MoveTo(ReadOnlySpan<byte> key)
        {
            _node = _sl.FindGreaterOrEqual(key, out _);
        }

        public void MoveNext()
        {
            if (_node != null)
            {
                _node = _node.Next[0];
            }
        }
    }
}
