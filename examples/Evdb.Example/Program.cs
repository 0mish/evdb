using Evdb;

using Store store = new("db");

RecordStream stream = store.Get("object-1");
stream.Append("info", new InfoEvent("Test"));
stream.Append("number", new NumberEvent(1));

RecordStream.Iterator iter = stream.GetIterator();

Console.WriteLine(iter.Record.Decode<InfoEvent>());
iter.MoveNext();
Console.WriteLine(iter.Record.Decode<NumberEvent>());
iter.MoveNext();

record class InfoEvent(string Message);
record class NumberEvent(int Value);
