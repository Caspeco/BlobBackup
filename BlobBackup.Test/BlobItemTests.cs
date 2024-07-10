using System.Globalization;
using Xunit;

namespace BlobBackup.Test;

public class BlobItemTests
{
    [Theory]
    [InlineData(@"test\\path:data\\file", @"test\\path--COLON--data\\file")]
    [InlineData(@"test\\<path>\\file", @"test\\--LT--path--GT--\\file")]
    [InlineData("x'y", "x'y")]
    [InlineData("x\"y", "x--QUOTE--y")] // OtherPunctuation ", u0022
    [InlineData("x<y", "x--LT--y")] // MathSymbol <, u003C
    [InlineData("x>y", "x--GT--y")] // MathSymbol >, u003E
    [InlineData("x|y", "x--PIPE--y")] // MathSymbol |, u007C
    [InlineData("x:y", "x--COLON--y")] // OtherPunctuation :, u003A
    [InlineData("x*y", "x--STAR--y")] // OtherPunctuation *, u002A
    [InlineData("x?y", "x--QUESTIONMARK--y")] // OtherPunctuation ?, u003F
    public void GetValidCharsPathTest(string input, string expected)
    {
        Assert.Equal(expected, BlobItem.GetValidCharsPath(input));
    }

    [Fact]
    public void GenerateCases()
    {
        foreach (var c in System.IO.Path.GetInvalidFileNameChars())
        {
            if (char.IsControl(c))
                continue;
            Console.WriteLine($"[InlineData(\"x{(char.IsControl(c) ? $"\\u{(int)c:X4}" : $"{c:c}")}y\", \"x--u{(int)c:X4}--y\")] // {CharUnicodeInfo.GetUnicodeCategory(c)} {(char.IsWhiteSpace(c) ? "" : $"{c:c}, ")}u{(int)c:X4}");
        }
    }
}
