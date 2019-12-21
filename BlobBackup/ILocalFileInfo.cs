using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

    public class LocalFileInfoDisk : ILocalFileInfo
    {
        internal readonly System.IO.FileInfo fInfo;

        public bool Exists => fInfo.Exists;
        public long Size => Exists ? fInfo.Length : -1;
        private string _md5;
        public string MD5 => _md5;
        public DateTime LastModifiedTimeUtc => fInfo.LastWriteTimeUtc;

        public LocalFileInfoDisk(string fileName)
        {
            fInfo = new System.IO.FileInfo(fileName);
        }

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
            using (var md5 = System.Security.Cryptography.MD5.Create())
            using (var stream = fInfo.OpenRead())
            {
                _md5 = Convert.ToBase64String(md5.ComputeHash(stream));
            }
            return _md5;
        }

        /// <summary>Set File write time to time in UTC</summary>
        public void UpdateWriteTime(DateTime lastModifiedTimeUtc)
        {
            if (Exists && LastModifiedTimeUtc != lastModifiedTimeUtc)
                System.IO.File.SetLastWriteTimeUtc(fInfo.FullName, lastModifiedTimeUtc);
        }
    }
}
