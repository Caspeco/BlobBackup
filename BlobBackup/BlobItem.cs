using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobBackup
{
    public class BlobItem
    {
        private readonly CloudBlockBlob Blob;
        public readonly Uri Uri;
        public readonly long Size;
        public readonly DateTimeOffset? LastModified;
        internal Func<string, System.IO.FileMode, Task> DownloadToFileAsync;

        private BlobItem(CloudBlockBlob blob)
        {
            Blob = blob;
            Uri = blob.Uri;
            Size = blob.Properties.Length;
            LastModified = blob.Properties.LastModified;
            DownloadToFileAsync = Blob.DownloadToFileAsync;
        }

        private static BlobItem GetBlobItem(IListBlobItem blobItem)
        {
            var blob = blobItem as CloudBlockBlob;
            if (blob == null)
                return null;
            return new BlobItem(blob);
        }

        public static IEnumerable<BlobItem> BlobEnumerator(string containerName, string accountName, string accountKey)
        {
            var account = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net");
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            foreach (IListBlobItem blobItem in container.ListBlobs(null, true, BlobListingDetails.None))
            {
                yield return GetBlobItem(blobItem);
            }
        }
    }
}
