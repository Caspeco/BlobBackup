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
        DateTime LastWriteTimeUtc { get; }
    }

    public class LocalFileInfoDisk : ILocalFileInfo
    {
        private readonly System.IO.FileInfo fInfo;

        public bool Exists => fInfo.Exists;
        public long Size => fInfo.Length;
        private string _md5;
        public string MD5 => _md5;
        public DateTime LastWriteTimeUtc => fInfo.LastWriteTimeUtc;

        public LocalFileInfoDisk(string fileName)
        {
            fInfo = new System.IO.FileInfo(fileName);
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
    }
}
