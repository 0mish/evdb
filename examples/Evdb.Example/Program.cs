using Evdb;

using Store store = new("db");

RecordStream stream = store.Get("object-1");
stream.Append(new InfoEvent("Test"));
stream.Append(new NumberEvent(1));

RecordStream.Iterator iter = stream.GetIterator();

Console.WriteLine(iter.Record.Deserialize<InfoEvent>());
iter.MoveNext();
Console.WriteLine(iter.Record.Deserialize<NumberEvent>());
iter.MoveNext();

record class InfoEvent(string Message);
record class NumberEvent(int Value);
