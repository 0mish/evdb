﻿using Evdb.Indexing.Lsm;
using System.Collections.Concurrent;
using System.Text;

namespace Evdb;

public sealed class Store : IDisposable
{
    private bool _disposed;
    private readonly LsmIndex _index;
    private readonly ConcurrentDictionary<string, RecordStream> _streams;

    public Store(string path)
    {
        LsmIndexOptions options = new()
        {
            Path = path
        };

        _index = new LsmIndex(options);
        _streams = new ConcurrentDictionary<string, RecordStream>();
    }

    public RecordStream All()
    {
        throw new NotImplementedException();
    }

    public RecordStream Get(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        return _streams.GetOrAdd(name, key => new RecordStream(_index, name));
    }

    public bool Exists(string name)
    {
        ArgumentNullException.ThrowIfNull(name, nameof(name));

        ReadOnlySpan<byte> key = Encoding.UTF8.GetBytes(name);

        return _index.TryGet(key, out _);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _index.Dispose();
        _disposed = true;
    }
}
