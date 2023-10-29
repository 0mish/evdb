using Evdb.Collections;
using NUnit.Framework.Internal;

namespace Evdb.Tests.Collections;

public class SkipListTests
{
    private SkipList _skipList;

    [SetUp]
    public void SetUp()
    {
        _skipList = new SkipList();
    }

    [Test]
    public void Set__Single__ValueSet()
    {
        _skipList.Set("a"u8, "value"u8);

        Assert.That(_skipList.Count, Is.EqualTo(1));
    }

    [Test]
    public void Set__Multiple__ValueSet()
    {
        _skipList.Set("a"u8, "value-a"u8);
        _skipList.Set("b"u8, "value-b"u8);

        Assert.That(_skipList.Count, Is.EqualTo(2));
    }

    [Test]
    public void Set__Multiple__ItemsOrdered()
    {
        Dictionary<byte[], byte[]> kvs = Generator.RandomKeyValues(128);

        foreach (KeyValuePair<byte[], byte[]> kv in kvs)
        {
            _skipList.Set(kv.Key, kv.Value);
        }

        AssertSequential(kvs);
    }

    [Test]
    public void Set__Concurrent_Multiple__ItemsOrdered()
    {
        const int PerCore = 128;

        Dictionary<byte[], byte[]> kvs = Generator.RandomKeyValues(PerCore * Environment.ProcessorCount);
        List<List<KeyValuePair<byte[], byte[]>>> pkvs = kvs
            .Select((k, i) => (k, i))
            .GroupBy(ki => ki.i / PerCore, ki => ki.k)
            .Select(g => g.ToList())
            .ToList();
        Task[] tasks = new Task[Environment.ProcessorCount];

        for (int i = 0; i < tasks.Length; i++)
        {
            List<KeyValuePair<byte[], byte[]>> ckvs = pkvs[i];

            tasks[i] = Task.Run(() =>
            {
                foreach (KeyValuePair<byte[], byte[]> kv in ckvs)
                {
                    _skipList.Set(kv.Key, kv.Value);
                }
            });
        }

        Task.WaitAll(tasks);

        AssertSequential(kvs);
    }

    [Test]
    public void TryGet__NotExist__ReturnsFalse()
    {
        bool result = _skipList.TryGet("a"u8, out ReadOnlySpan<byte> value);

        Assert.That(result, Is.False);
        Assert.That(value.Length, Is.EqualTo(0));
    }

    [Test]
    public void TryGet__Exist__ReturnsTrue()
    {
        _skipList.Set("a"u8, "value-a"u8);

        bool result = _skipList.TryGet("a"u8, out ReadOnlySpan<byte> value);

        Assert.That(result, Is.True);
        Assert.That(value.ToArray(), Is.EqualTo("value-a"u8.ToArray()));
    }

    [Test]
    public void TryGet__Multiple__ReturnsTrue()
    {
        Dictionary<byte[], byte[]> kvs = Generator.RandomKeyValues(128);

        foreach (KeyValuePair<byte[], byte[]> kv in kvs)
        {
            _skipList.Set(kv.Key, kv.Value);
        }

        foreach (KeyValuePair<byte[], byte[]> kv in kvs)
        {
            bool found = _skipList.TryGet(kv.Key, out ReadOnlySpan<byte> value);

            Assert.That(found, Is.True);
            Assert.That(value.ToArray(), Is.EqualTo(kv.Value));
        }
    }

    private void AssertSequential(Dictionary<byte[], byte[]> expectation)
    {
        int count = 0;
        byte[]? prevKey = null;
        byte[] currKey;
        SkipList.Iterator iter = _skipList.GetIterator();

        for (iter.MoveToFirst(); iter.IsValid; iter.MoveNext())
        {
            currKey = iter.Key.ToArray();

            Assert.That(expectation[currKey], Is.EqualTo(iter.Value.ToArray()));
            Assert.That(prevKey.AsSpan().SequenceCompareTo(currKey), Is.LessThan(0));

            prevKey = currKey;
            count++;
        }

        Assert.That(count, Is.EqualTo(expectation.Count));
    }
}
