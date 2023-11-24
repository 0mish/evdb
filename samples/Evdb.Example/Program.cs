using Evdb;
using Evdb.Storage.RocksDb;

StoreOptions options = new()
{
    Database = new Database(new DatabaseOptions
    {
        Path = "rocks-db"
    })
};

using Store store = new(options);

RecordStream stream1 = store.Get("object-1");
stream1.Append(new InfoEvent("event-11"));
stream1.Append(new InfoEvent("event-12"));

RecordStream stream2 = store.Get("object-2");
stream2.Append(new InfoEvent("event-21"));
stream2.Append(new InfoEvent("event-22"));

Console.WriteLine($"For {stream1.Name}");

#if false
foreach (BoxedRecord record in stream1.GetIterator())
{
    Console.WriteLine($" {record.Deserialize<InfoEvent>()}");
}

Console.WriteLine($"For {stream2.Name}");

foreach (BoxedRecord record in stream2.GetIterator())
{
    Console.WriteLine($" {record.Deserialize<InfoEvent>()}");
}
#endif

record class InfoEvent(string Message);
record class NumberEvent(int Value);
