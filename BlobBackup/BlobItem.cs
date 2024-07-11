using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace BlobBackup
{
    public class BlobItem : ILocalFileInfo
    {
        private readonly Azure.Storage.Blobs.Models.BlobItem Blob;
        public string Name { get; }

        #region ILocalFileInfo
        public bool Exists => !Blob.Deleted;
        public long Size { get; private set; }
        public string MD5 { get; private set; }
        public DateTime LastModifiedTimeUtc => LastModifiedUtc.UtcDateTime;
        #endregion ILocalFileInfo

        public DateTimeOffset LastModifiedUtc { get; private set; }
        internal Func<FileInfo, Task> DownloadToFileAsync;

        private void UpdateProps(BlobItemProperties props)
        {
            Size = props.ContentLength ?? -1;
            MD5 = Convert.ToBase64String(props.ContentHash);
            LastModifiedUtc = props.LastModified ?? DateTimeOffset.MinValue;
        }

        private BlobItem(Azure.Storage.Blobs.Models.BlobItem blob, BlobContainerClient cli)
        {
            Blob = blob;
            Name = $"/{cli.Name}/{blob.Name}";
            UpdateProps(blob.Properties);
            DownloadToFileAsync = async (FileInfo fi) => await cli.GetBlobClient(blob.Name).DownloadToAsync(fi.FullName);
        }

        private static readonly char[] InvalidPathChars = System.IO.Path.GetInvalidFileNameChars().Where(c => c != '\\').ToArray();
        private static string GetCharReplacement(char c) =>
            c switch
            {
                '"' => "QUOTE",
                '<' => "LT",
                '>' => "GT",
                '|' => "PIPE",
                ':' => "COLON",
                '*' => "STAR",
                '?' => "QUESTIONMARK",
                _ => null,
            };

        /// <summary>Convert path with possible invalid chars to --CHAR-- alternative</summary>
        public static string GetValidCharsPath(string path)
        {
            int idx = 0;
            while ((idx = path.IndexOfAny(InvalidPathChars, idx + 1)) != -1)
            {
                var problemChar = path[idx];
                var replacement = GetCharReplacement(problemChar)
                    ?? throw new Exception($"Filename {path} contains invalid char { problemChar} @{idx} = {System.Globalization.CharUnicodeInfo.GetUnicodeCategory(problemChar)} and we have not replacement");
                path = path.Replace($"{problemChar}", $"--{replacement}--");
            }

            return path;
        }

        public string GetLocalFileName() => GetValidCharsPath(Name.Replace("//", "/").Replace('/', '\\').TrimStart('\\'));

        public override string ToString() => string.Join("|", Name, Size, LastModifiedUtc, MD5);

        public static async IAsyncEnumerable<ParallelQuery<T>> BlobEnumeratorAsync<T>(string containerName, string accountName, string accountKey, Func<(long, BlobItem), T> getItem)
        {
            var cli = new BlobContainerClient($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net", containerName);

            (long, BlobItem) GetBlobItem(Azure.Storage.Blobs.Models.BlobItem blob) => (blob.Properties.ContentLength ?? 0, new(blob, cli));

            var prefixes = new HashSet<string>();
            await foreach (var page in cli
                .GetBlobsByHierarchyAsync(delimiter: "/")
                .AsPages(pageSizeHint: 20000))
            {
                foreach (var p in page.Values)
                {
                    if (p.Blob is null && p.Prefix is not null)
                    {
                        prefixes.Add(p.Prefix);
                    }
                }
                var blobs = page.Values.Select(p => p.Blob).Where(b => b is not null);
                if (blobs.Any())
                {
                    yield return blobs.AsParallel().Select(GetBlobItem).Select(getItem);
                }
            }

            foreach (var pages in prefixes.AsParallel()
                .Select(pfx => cli.GetBlobsAsync(prefix: pfx).AsPages(pageSizeHint: 20000)))
            {
                await foreach (var page in pages)
                {
                    yield return page.Values.AsParallel().Select(GetBlobItem).Select(getItem);
                }
            }
        }
    }
}
