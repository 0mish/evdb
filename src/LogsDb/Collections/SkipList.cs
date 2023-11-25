using LogsDb.Memory;

namespace LogsDb.Collections;

internal unsafe sealed class SkipList : IDisposable
{
    private const int MaxHeight = 13;

    private bool _disposed;

    private int _height;
    private readonly Node* _head;
    private readonly Random _rand;

    private readonly Arena _nodeArena;
    private readonly Arena _keyArena;
    private readonly Arena _valueArena;

    public int Count { get; private set; }

    public SkipList()
    {
        _nodeArena = new Arena();
        _keyArena = new Arena();
        _valueArena = new Arena();

        _height = 0;
        _head = NewNode(default, default, MaxHeight);
        _rand = new Random(0);

    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Node** prevs = stackalloc Node*[MaxHeight];

        FindGreaterOrEqual(key, prevs);

        int height = RandomHeight();

        if (height > _height)
        {
            for (int i = _height; i < height; i++)
            {
                prevs[i] = _head;
            }

            _height = height;
        }

        Node* node = NewNode(key, value, height);

        for (int i = 0; i < height; i++)
        {
            node->Next[i] = prevs[i]->Next[i];
            prevs[i]->Next[i] = node;
        }

        Count++;
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        Node* node = FindGreaterOrEqual(key);

        // TODO: optimize - Avoid a second sequence compare.
        if (node != null && key.SequenceCompareTo(node->Key) == 0)
        {
            value = node->Value;

            return true;
        }

        value = default;

        return false;
    }

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    private Node* FindFirst()
    {
        return _head->Next[0];
    }

    private Node* FindGreaterOrEqual(ReadOnlySpan<byte> key, Node** prevs = null)
    {
        Node* node = _head;
        Node* next = null;

        for (int i = _height - 1; i >= 0; i--)
        {
            next = node->Next[i];

            while (next != null && key.SequenceCompareTo(next->Key) > 0)
            {
                node = next;
                next = next->Next[i];
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

    private Node* NewNode(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, int height)
    {
        Node* node = _nodeArena.Allocate<Node>();

        node->Height = height;
        node->Next = (Node**)_nodeArena.Allocate<nuint>(height, alignment: 1);

        node->KeyLength = key.Length;
        node->KeyPointer = _keyArena.Allocate<byte>(key.Length);

        node->ValueLength = value.Length;
        node->ValuePointer = _keyArena.Allocate<byte>(value.Length);

        key.CopyTo(new Span<byte>(node->KeyPointer, node->KeyLength));
        value.CopyTo(new Span<byte>(node->ValuePointer, node->ValueLength));

        new Span<nuint>(node->Next, height).Clear();

        return node;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _nodeArena.Dispose();
        _valueArena.Dispose();
        _keyArena.Dispose();

        _disposed = true;
    }

    private struct Node
    {
        // TODO: Optimize `Next` field away.
        public Node** Next;
        public byte* KeyPointer;
        public byte* ValuePointer;
        public int KeyLength;
        public int ValueLength;
        public int Height;

        public readonly ReadOnlySpan<byte> Key => new(KeyPointer, KeyLength);
        public readonly ReadOnlySpan<byte> Value => new(ValuePointer, ValueLength);
    }

    public struct Iterator
    {
        private Node* _node;
        private readonly SkipList _sl;

        public readonly ReadOnlySpan<byte> Key => _node != null ? _node->Key : default;
        public readonly ReadOnlySpan<byte> Value => _node != null ? _node->Value : default;
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
            _node = _sl.FindGreaterOrEqual(key);
        }

        public void MoveNext()
        {
            if (_node != null)
            {
                _node = _node->Next[0];
            }
        }
    }
}
