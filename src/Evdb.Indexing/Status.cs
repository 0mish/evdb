namespace Evdb;

public enum StatusCode
{
    // Status codes after this are considered successful.
    Success,
    Found,
    NotFound,
    EoF,

    // Status codes after this are considered failures.
    Failed,
    Filled,
    Disposed,
    Closed,
    Corrupted
}

public readonly struct Status
{
    public StatusCode Code { get; }
    public string? Message { get; }

    public bool IsSuccess => Code < StatusCode.Failed;
    public bool IsFound => Code == StatusCode.Found;
    public bool IsNotFound => Code == StatusCode.NotFound;
    public bool IsEoF => Code == StatusCode.EoF;

    public Status(StatusCode code, string? message = null)
    {
        Code = code;
        Message = message;
    }

    internal static Status Success => new(StatusCode.Success);
    internal static Status Found => new(StatusCode.Found);
    internal static Status NotFound => new(StatusCode.NotFound);
    internal static Status EoF => new(StatusCode.EoF);

    internal static Status Failed => new(StatusCode.Failed);
    internal static Status Filled => new(StatusCode.Filled);
    internal static Status Closed => new(StatusCode.Closed);
    internal static Status Disposed => new(StatusCode.Disposed);
    internal static Status Corrupted => new(StatusCode.Corrupted);
}
