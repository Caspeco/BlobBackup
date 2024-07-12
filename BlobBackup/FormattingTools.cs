
namespace BlobBackup;

public static class FormattingTools
{
    private enum FileSizeUnit : byte
    {
        B, KB, MB,
        GB, TB, PB,
        EB, ZB, YB,
    }

    public static string FormatSize(this double size)
    {
        var unit = FileSizeUnit.B;
        while (size >= 1024 && unit < FileSizeUnit.YB)
        {
            size = size / 1024;
            unit++;
        }
        return $"{size:#,##0.##} {unit}";
    }

    public static string FormatSize(this long size) => FormatSize((double) size);

    public static string Format(this int value) => $"{value:#,##0}";
}
