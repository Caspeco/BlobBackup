
using System.Text;

namespace BlobBackup
{
    public interface ILocalFileInfo
    {
        bool Exists { get; }
        long Size { get; }
        string MD5 { get; }
        DateTime LastModifiedTimeUtc { get; }
    }

    public static class ILocalFileInfoExtensions
    {
        public static bool IsSame(this ILocalFileInfo x, ILocalFileInfo y)
        {
            return
                x.Exists == y.Exists &&
                x.Size == y.Size &&
                x.MD5 == y.MD5 &&
                x.LastModifiedTimeUtc == y.LastModifiedTimeUtc &&
                true;
        }

        public static string DiffString(this ILocalFileInfo x, ILocalFileInfo y)
        {
            var sb = new StringBuilder();
            sb.Append($"Exists: {x.Exists}");
            if (x.Exists != y.Exists) sb.Append($" vs {y.Exists}");

            sb.Append($", Size: {x.Size}");
            if (x.Size != y.Size) sb.Append($" vs {y.Size}");

            sb.Append($", MD5: {x.MD5}");
            if (x.MD5 != y.MD5) sb.Append($" vs {y.MD5}");

            sb.Append($", LasModifiedUtc: {x.LastModifiedTimeUtc}");
            if (x.LastModifiedTimeUtc != y.LastModifiedTimeUtc) sb.Append($" vs {y.LastModifiedTimeUtc}");

            return sb.ToString();
        }
    }

    public class LocalFileInfoDisk(FileInfo fInfo) : ILocalFileInfo
    {
        internal FileInfo FileInfo => fInfo;

        public bool Exists => FileInfo.Exists;
        public long Size => Exists ? FileInfo.Length : -1;
        private string _md5;
        public string MD5 => _md5;
        public DateTime LastModifiedTimeUtc => FileInfo.LastWriteTimeUtc;

        /// <summary>
        /// Returns calculated MD5 if possible
        /// </summary>
        public string GetMd5()
        {
            if (!Exists)
                return MD5;
            if (string.IsNullOrEmpty(MD5))
                CalculateMd5();
            return MD5;
        }

        public string CalculateMd5()
        {
            using var stream = new FileStream(FileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, true);
            return _md5 = Convert.ToBase64String(System.Security.Cryptography.MD5.HashData(stream));
        }

        /// <summary>Set File write time to time in UTC</summary>
        public void UpdateWriteTime(DateTime lastModifiedTimeUtc)
        {
            FileInfo.Refresh();
            if (Exists && LastModifiedTimeUtc != lastModifiedTimeUtc)
            {
                System.IO.File.SetLastWriteTimeUtc(FileInfo.FullName, lastModifiedTimeUtc);
                FileInfo.Refresh();
            }
        }

        public void Delete()
        {
            if (!Exists)
                FileInfo.Refresh();
            if (!Exists)
                return;
            FileInfo.Delete();
            FileInfo.Refresh();
        }
    }
}
