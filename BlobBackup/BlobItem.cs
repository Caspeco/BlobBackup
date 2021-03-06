﻿using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobBackup
{
    public class BlobItem : ILocalFileInfo
    {
        private readonly CloudBlockBlob Blob;
        public Uri Uri { get; }

        #region ILocalFileInfo
        public bool Exists => !Blob.IsDeleted;
        public long Size { get; }
        public string MD5 { get; }
        public DateTime LastModifiedTimeUtc => LastModifiedUtc.UtcDateTime;
        #endregion ILocalFileInfo

        public DateTimeOffset LastModifiedUtc { get; }
        internal Func<string, System.IO.FileMode, Task> DownloadToFileAsync;

        private BlobItem(CloudBlockBlob blob)
        {
            Blob = blob;
            Uri = blob.Uri;
            Size = blob.Properties.Length;
            MD5 = blob.Properties.ContentMD5;
            LastModifiedUtc = blob.Properties.LastModified ?? DateTimeOffset.MinValue;
            DownloadToFileAsync = Blob.DownloadToFileAsync;
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

        public static ParallelQuery<BlobItem> BlobEnumerator(string containerName, string accountName, string accountKey)
        {
            var account = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net");
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            return container.ListBlobs(null, true, BlobListingDetails.None).AsParallel().Select(GetBlobItem);
        }
    }
}
