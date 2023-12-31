﻿using LogsDb.Formatting;
using LogsDb.IO;

namespace LogsDb.Tests.Indexing;

internal class PhysicalTableTests
{
    [TestCase(0)]
    [TestCase(16 * 1024)]
    public void PhysicalTable__Iterator__ElementPresent(int n)
    {
        Dictionary<byte[], byte[]> kvs = Generator.KeyValues(n, keySize: 4, valueSize: 1);
        VirtualTable vtable = new(null, long.MaxValue);

        foreach (KeyValuePair<byte[], byte[]> kv in kvs)
        {
            vtable.Set(kv.Key, kv.Value);
        }

        FileSystem fs = new();
        FileMetadata metadata = new(string.Empty, FileType.Table, 0);

        vtable.Flush(fs, metadata, dataBlockSize: 1024 * 16, bloomBlockSize: 1024 * 4);

        PhysicalTable ptable = new(fs, metadata, new WeakReferenceBlockCache());

        ptable.Open();

        PhysicalTable.Iterator iter = ptable.GetIterator();

        int count = 0;

        for (iter.MoveToFirst(); iter.IsValid; iter.MoveNext())
        {
            byte[] key = iter.Key.ToArray();

            Assert.That(kvs, Contains.Key(key));
            Assert.That(kvs[key], Is.EqualTo(iter.Value.ToArray()));

            count++;
        }

        Assert.That(count, Is.EqualTo(n));
    }
}
