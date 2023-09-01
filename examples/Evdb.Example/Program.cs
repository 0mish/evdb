using Evdb;

using Store store = new("db");

RecordStream stream = store.Get("object-1");
stream.AppendJson(new InfoEvent("Test"));
stream.AppendJson(new NumberEvent(1));

record class InfoEvent(string Data);
record class NumberEvent(int Data);
