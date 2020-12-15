using System;
using System.IO;

namespace DuplicateFinder
{
    public class DupItem
    {
        public readonly FileInfo FileInfo;

        public bool Exists => FileInfo.Exists;
        public long Size => Exists ? FileInfo.Length : -1;
        private readonly object _hardLinksLock = new object();
        private string[] _allHardLinks;
        public bool HasHardLink => GetHardLinks(null).Length >= 2;
        public bool IsReparsePoint => FileInfo.Attributes.HasFlag(FileAttributes.ReparsePoint);
        public string Md5Full { get; private set; }
        public string Md5OneBlock { get; private set; }
        public DateTime LastModifiedTimeUtc => FileInfo.LastWriteTimeUtc;

        public DupItem(FileInfo fInfo)
        {
            FileInfo = fInfo;
        }

        public string[] GetHardLinks(string stripPath)
        {
            var links = _allHardLinks;
            if (links == null)
                lock (_hardLinksLock)
                    links = _allHardLinks ?? (_allHardLinks = HardLinkHelper.GetHardLinks(FileInfo.FullName));

            if (string.IsNullOrEmpty(stripPath))
                return links;

            for (int i = 0; i < links.Length; i++)
            {
                if (links[i].StartsWith(stripPath))
                    links[i] = links[i].Substring(stripPath.Length);
            }
            _allHardLinks = links;
            return links;
        }

        private const string ZeroSizeMd5AsBase64 = "1B2M2Y8AsgTpgAmY7PhCfg==";

        /// <summary>
        /// Returns calculated MD5 if possible
        /// </summary>
        public string GetMd5Full()
        {
            if (!Exists)
                return Md5Full;
            if (!string.IsNullOrEmpty(Md5Full))
                return Md5Full;
            if (Size == 0)
                return ZeroSizeMd5AsBase64;

            using (var md5 = System.Security.Cryptography.MD5.Create())
            using (var stream = FileInfo.OpenRead())
            {
                return Md5Full = Convert.ToBase64String(md5.ComputeHash(stream));
            }
        }

        private const int OneBlock = 4 * 1024;
        public string GetMd5OneBlock()
        {
            if (!Exists)
                return Md5OneBlock;
            if (Size <= 8 * 1024)
                return Md5OneBlock = GetMd5Full();
            if (!string.IsNullOrEmpty(Md5OneBlock))
                return Md5OneBlock;

            using (var md5 = System.Security.Cryptography.MD5.Create())
            using (var stream = FileInfo.OpenRead())
            {
                var buf = new byte[OneBlock];
                stream.Read(buf, 0, OneBlock);
                return Md5OneBlock = Convert.ToBase64String(md5.ComputeHash(buf));
            }
        }

        public override string ToString()
        {
            return $"{Size} {GetMd5OneBlock()} {Md5Full} {FileInfo.FullName}";
        }
    }

}
