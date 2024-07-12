namespace BlobBackup;

public class ItemCountSize
{
    public int Count;
    public long Size;

    public (int count, long size) Add(long size) =>
        (Interlocked.Increment(ref Count),
        Interlocked.Add(ref Size, size));

    public override string ToString() => $"{Count:#,##0} ({Size.FormatSize()})";
}
