
using Xunit;

namespace BlobBackup.Test
{
    public class FormatSizeTests
    {
        [Theory]
        [InlineData(1, "1 B")]
        [InlineData(1000, "1 000 B")]
        [InlineData(1024, "1 KB")]
        [InlineData(1024 + 512, "1,5 KB")]
        [InlineData(1024 * 1024 * 1024, "1 GB")]
        [InlineData(long.MaxValue, "8 EB")]
        [InlineData(long.MaxValue / 3, "2,67 EB")]
        [InlineData(ulong.MaxValue, "16 EB")]
        [InlineData(ulong.MaxValue / 3, "5,33 EB")]
        [InlineData(double.MaxValue, "148 701 690 847 778 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 000 YB")]
        public void SimpleTest(double size, string expectedString)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("sv-SE");
            var result = size.FormatSize();
            Console.WriteLine("Len: " + result.Length + " data: " + result);
            Assert.Equal(expectedString, result);
        }

        [Theory]
        [InlineData(1234567890, "1 234 567 890")]
        [InlineData(1, "1")]
        [InlineData(1000, "1 000")]
        public void IntFormatTest(int value, string expected)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("sv-SE");
            Assert.Equal(expected, value.Format());
        }
    }
}
