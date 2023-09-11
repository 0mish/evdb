using Evdb;

using Store store = new("db");

RecordStream stream = store.Get("object-1");
stream.Append("info", new InfoEvent("Test"));
stream.Append("number", new NumberEvent(1));

record class InfoEvent(string Data);
record class NumberEvent(int Data);
