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

    [TearDown]
    public void TearDown()
    {
        _skipList.Dispose();
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
        List<KeyValuePair<byte[], byte[]>> kvs = new();

        Random rand = new(0);

        for (int i = 0; i < 128; i++)
        {
            byte[] key = new byte[16];
            byte[] value = new byte[16];

            rand.NextBytes(key);
            rand.NextBytes(value);

            kvs.Add(new(key, value));

            _skipList.Set(key, value);
        }

        foreach (KeyValuePair<byte[], byte[]> kv in kvs)
        {
            bool found = _skipList.TryGet(kv.Key, out ReadOnlySpan<byte> value);

            Assert.That(found, Is.True);
            Assert.That(value.ToArray(), Is.EqualTo(kv.Value));
        }

        int count = 0;
        SkipList.Iterator iter = _skipList.GetIterator();

        for (iter.MoveToFirst(); iter.IsValid; iter.MoveNext())
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(128));
    }
}
