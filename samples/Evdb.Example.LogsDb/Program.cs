using LogsDb;

unsafe
{
    const int Count = 100000;

    DatabaseOptions opt = new()
    {
        Path = "db"
    };

    using (Database db = new(opt))
    {
        db.Open();

        for (long i = 0; i < Count; i++)
        {
            ReadOnlySpan<byte> key = new(&i, sizeof(long));
            ReadOnlySpan<byte> value = key;

            Status status = db.Set(key, value);

            if (!status.IsSuccess)
            {
                throw new Exception($"Status failure {status.Code}.");
            }
        }
    }

    using (Database db = new(opt))
    {
        db.Open();

        for (long i = 0; i < Count; i++)
        {
            ReadOnlySpan<byte> key = new(&i, sizeof(long));

            Status status = db.Get(key, out ReadOnlySpan<byte> value);

            if (!status.IsSuccess)
            {
                throw new Exception($"Status failure {status.Code}.");
            }
            else if (status.IsNotFound)
            {
                Console.WriteLine($"{i} not found.");
            }
        }
    }
}
