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

        public string GetLocalFileName() => Name.Replace("//", "/").Replace('/', '\\').TrimStart('\\').Replace(":", "--COLON--");

        public override string ToString() => string.Join("|", Name, Size, LastModifiedUtc, MD5);

        public static async IAsyncEnumerable<ParallelQuery<T>> BlobEnumeratorAsync<T>(string containerName, string accountName, string accountKey, Func<(long, BlobItem), T> getItem)
        {
            var cli = new BlobContainerClient($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net", containerName);

            (long, BlobItem) GetBlobItem(Azure.Storage.Blobs.Models.BlobItem blob) => (blob.Properties.ContentLength ?? 0, new(blob, cli));

            await foreach (var page in cli.GetBlobsAsync().AsPages())
            {
                yield return page.Values.AsParallel().Select(GetBlobItem).Select(getItem);
            }
        }
    }
}
