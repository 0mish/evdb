namespace Evdb.Collections;

internal sealed class SkipList<TKey, TValue>
{
    private int _level;
    private Node _head;
    private readonly Random _rand;

    public int Count { get; private set; }

    public SkipList()
    {
        _rand = new Random();
        _level = -1;
        _head = new Node(default!, default, 0);
    }

    public void Add(TKey key, TValue? value)
    {
        int level = RandomLevel();
        Node node;
        Node? next;

        if (level > _level)
        {
            node = _head;

            _head = new Node(default!, default, level);
            _level = level;

            for (int i = 0; i < node.Forwards.Length; i++)
            {
                _head.Forwards[i] = node.Forwards[i];
            }
        }

        Node[] nodes = new Node[_level + 1];
        node = _head;

        for (int i = _level; i >= 0; i--)
        {
            next = node.Forwards[i];

            while (next != null && Comparer<TKey>.Default.Compare(key, next.Key) < 0)
            {
                node = next;
                next = next.Forwards[i];
            }

            nodes[i] = node;
        }

        node = new Node(key, value, level);

        for (int i = 0; i <= level; i++)
        {
            node.Forwards[i] = nodes[i].Forwards[i];
            nodes[i].Forwards[i] = node;
        }

        Count++;
    }

    public bool Remove(TKey key)
    {
        Node[] nodes = new Node[_level + 1];
        Node node = _head;
        Node? next;

        for (int i = _level; i >= 0; i--)
        {
            next = node.Forwards[i];

            while (next != null && Comparer<TKey>.Default.Compare(key, next.Key) < 0)
            {
                node = next;
                next = next.Forwards[i];
            }

            nodes[i] = node;
        }

        next = node.Forwards[0];

        if (next == null || Comparer<TKey>.Default.Compare(key, next.Key) != 0)
        {
            return false;
        }

        for (int i = 0; i < next.Forwards.Length; i++)
        {
            nodes[i].Forwards[i] = next.Forwards[i];
        }

        Count--;

        return true;
    }

    public bool TryGetValue(TKey key, out TValue? value)
    {
        Node node = _head;
        Node? next;

        for (int i = _level; i >= 0; i--)
        {
            next = node.Forwards[i];

            while (next != null && Comparer<TKey>.Default.Compare(key, next.Key) < 0)
            {
                node = next;
                next = next.Forwards[i];
            }
        }

        next = node.Forwards[0];

        if (next != null && Comparer<TKey>.Default.Compare(key, next.Key) == 0)
        {
            value = next.Value;

            return true;
        }

        value = default;

        return false;
    }

    private int RandomLevel()
    {
        int result = 0;

        while (_rand.Next() % 2 == 0)
        {
            result++;
        }

        return result;
    }

    private class Node
    {
        public TKey Key { get; set; }
        public TValue? Value { get; set; }
        public Node?[] Forwards { get; set; }

        public Node(TKey key, TValue? value, int level)
        {
            Key = key;
            Value = value;
            Forwards = new Node[level + 1];
        }
    }
}
