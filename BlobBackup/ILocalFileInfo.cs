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
        DateTime LastWriteTimeUtc { get; }
    }

    public class LocalFileInfoDisk : ILocalFileInfo
    {
        private readonly System.IO.FileInfo fInfo;

        public bool Exists => fInfo.Exists;
        public long Size => fInfo.Length;
        public DateTime LastWriteTimeUtc => fInfo.LastWriteTimeUtc;

        public LocalFileInfoDisk(string fileName)
        {
            fInfo = new System.IO.FileInfo(fileName);
        }
    }
}
