using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobBackup
{
    public class BlobItem : ILocalFileInfo
    {
        private readonly CloudBlockBlob Blob;
        public Uri Uri { get; }

        #region ILocalFileInfo
        public bool Exists => !Blob.IsDeleted;
        public long Size { get; private set; }
        public string MD5 { get; private set; }
        public DateTime LastModifiedTimeUtc => LastModifiedUtc.UtcDateTime;
        #endregion ILocalFileInfo

        public DateTimeOffset LastModifiedUtc { get; private set; }
        internal Func<string, System.IO.FileMode, Task> DownloadToFileAsync;

        private void UpdateProps(BlobProperties props)
        {
            // TODO cache on first use only
            Size = props.Length;
            MD5 = props.ContentMD5;
            LastModifiedUtc = props.LastModified ?? DateTimeOffset.MinValue;
        }

        private BlobItem(CloudBlockBlob blob)
        {
            Blob = blob;
            Uri = blob.Uri;
            UpdateProps(blob.Properties);
            DownloadToFileAsync = blob.DownloadToFileAsync;
        }

        public string GetLocalFileName()
        {
            return Uri.AbsolutePath.Replace("//", "/").Replace('/', '\\').Replace(":", "--COLON--").Substring(1);
        }

        public override string ToString()
        {
            return string.Join("|", Uri.AbsolutePath, Size, LastModifiedUtc, MD5);
        }

        private static BlobItem GetBlobItem(IListBlobItem blobItem)
        {
            if (!(blobItem is CloudBlockBlob blob))
                return null;
            return new BlobItem(blob);
        }

        public static async IAsyncEnumerable<ParallelQuery<T>> BlobEnumeratorAsync<T>(string containerName, string accountName, string accountKey, Func<BlobItem, T> getItem)
        {
            var account = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net");
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var list = new List<T>();
            BlobContinuationToken continuationToken = null;
            do
            {
                var response = await container.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                yield return response.Results.AsParallel().Select(GetBlobItem).Select(getItem);
            }
            while (continuationToken != null);

        }
    }
}
