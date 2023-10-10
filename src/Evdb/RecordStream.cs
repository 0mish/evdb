using Evdb.Indexing;
using System.Collections;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace Evdb;

[DebuggerDisplay("Name = {Name}")]
public sealed class RecordStream
{
    private int _count;
    private bool _hasCount;

    private readonly object _sync;
    private readonly byte[] _key;
    private readonly Database _db;

    public string Name { get; }

    internal RecordStream(Database db, string name)
    {
        _sync = new object();
        _db = db;
        _key = Encoding.UTF8.GetBytes(name);

        Name = name;
    }

    public void Append(Record record)
    {
        lock (_sync)
        {
            if (!_hasCount)
            {
                // FIXME: Find the max key prefixed with "_key".
                _count = 0;
                _hasCount = true;
            }

            ReadOnlySpan<byte> key = RecordKey.Encode(_key, _count);
            ReadOnlySpan<byte> value = Record.Encode(record.Type, record.Data);

            if (!_db.TrySet(key, value))
            {
                throw new Exception("Failed to append Record.");
            }

            _count++;
        }
    }

    public void Append<T>(T value)
    {
        using MemoryStream stream = new();

        JsonSerializer.Serialize(stream, value);

        ReadOnlySpan<byte> buffer = stream.GetBuffer();
        ReadOnlySpan<byte> data = buffer[..(int)stream.Length];
        ReadOnlySpan<byte> type = Encoding.UTF8.GetBytes(typeof(T).FullName!);
        Record record = new(type, data);

        Append(record);
    }

    public Iterator GetIterator()
    {
        return new Iterator(this);
    }

    public class Iterator : IEnumerable<BoxedRecord>
    {
        private readonly Database.Iterator _iter;
        private readonly RecordStream _stream;

        public Record Record => Valid() ? new Record(default, _iter.Value) : default;

        internal Iterator(RecordStream stream)
        {
            _stream = stream;
            _iter = stream._db.GetIterator();

            MoveToFirst();
        }

        public bool Valid()
        {
            return _iter.Valid() && _iter.Key.StartsWith(_stream._key);
        }

        public void MoveToFirst()
        {
            _iter.MoveTo(_stream._key);
        }

        public void MoveTo(int count)
        {
            ReadOnlySpan<byte> key = RecordKey.Encode(_stream._key, count);

            _iter.MoveTo(key);
        }

        public void MoveNext()
        {
            _iter.MoveNext();
        }

        public IEnumerator<BoxedRecord> GetEnumerator()
        {
            while (Valid())
            {
                yield return Record.Box();

                MoveNext();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
