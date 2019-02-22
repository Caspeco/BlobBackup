using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BlobBackup
{
    internal class Backup
    {
        private readonly string _localPath;
        private readonly string _containerName;

        public int ScannedItems = 0;
        public int IgnoredItems = 0;
        public int UpToDateItems = 0;
        public int NewItems = 0;
        public int ModifiedItems = 0;
        public int DeletedItems = 0;
        public long NewItemsSize = 0;
        public long ModifiedItemsSize = 0;

        private HashSet<string> ExpectedLocalFiles = new HashSet<string>();
        private RunQueue<BlobJob> BlobJobQueue = new RunQueue<BlobJob>();
        internal List<Task> Tasks = new List<Task>();

        public Backup(string localPath, string containerName)
        {
            _localPath = localPath;
            _containerName = containerName;
        }

        private const string FLAG_MODIFIED = "[MODIFIED ";
        private const string FLAG_DELETED = "[DELETED ";
        private const string FLAG_DATEFORMAT = "yyyyMMddHmm";
        private const string FLAG_END = "]";

        public Backup PrepareJob(string accountName, string accountKey, IProgress<int> progress)
        {
            var localContainerPath = Path.Combine(_localPath, _containerName);
            Directory.CreateDirectory(localContainerPath);

            var localFileListTask = Task.Run(() =>
            {
                // load list of local files
                return Directory.GetFiles(localContainerPath, "*", SearchOption.AllDirectories).
                Where(f => !f.Contains(FLAG_MODIFIED) && !f.Contains(FLAG_DELETED)).
                ToList();
            });

            try
            {
                foreach (var blob in BlobItem.BlobEnumerator(_containerName, accountName, accountKey))
                {
                    ScannedItems++;
                    try
                    {
                        if (ScannedItems % 10000 == 0)
                        {
                            progress.Report(ScannedItems);
                        }

                        if (blob == null)
                        {
                            IgnoredItems++;
                            continue;
                        }

                        var localFileName = blob.GetLocalFileName();
                        var bJob = new BlobJob(blob, Path.Combine(_localPath, localFileName));
                        ExpectedLocalFiles.Add(localFileName);

                        ILocalFileInfo file = new LocalFileInfoDisk(bJob.LocalFilePath);
                        bJob.FileInfo = file;
                        if (!file.Exists)
                        {
                            bJob.NeedsJob = JobType.New;
                            BlobJobQueue.AddDone(bJob);
                            NewItems++;
                            NewItemsSize += blob.Size;
                        }
                        else if (file.Size != blob.Size || file.LastWriteTimeUtc < blob.LastModifiedUtc.UtcDateTime ||
                            (file.MD5 != null && !string.IsNullOrEmpty(blob.MD5) && file.MD5 != blob.MD5))
                        {
                            bJob.NeedsJob = JobType.Modified;
                            BlobJobQueue.AddDone(bJob);
                            ModifiedItems++;
                            ModifiedItemsSize += blob.Size;
                        }
                        else
                        {
                            UpToDateItems++;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"INSIDE LOOP EXCEPTION while scanning {_containerName}. Item: {blob.Uri} Scanned Items: #{ScannedItems}. Ex message:" + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OUTER EXCEPTION ({_containerName}) #{ScannedItems}: " + ex.Message);
            }
            BlobJobQueue.RunnerDone();

            Tasks.Add(Task.Run(async () =>
            {
                // scan for deleted files by checking if we have a file locally that we did not find remotely
                var nowUtc = DateTime.UtcNow;
                foreach (var fileName in await localFileListTask)
                {
                    var localFilename = fileName;
                    if (localFilename.StartsWith(_localPath)) localFilename = localFilename.Substring(_localPath.Length + 1);
                    if (!ExpectedLocalFiles.Contains(localFilename))
                    {
                        Console.Write("D");
                        File.Move(fileName, fileName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END);
                        DeletedItems++;
                    }
                }
            }));

            return this;
        }

        public async Task ProcessJob(int simultaniousDownloads)
        {
            foreach (var item in BlobJobQueue.GetDoneEnumerable())
            {
                while (Tasks.Count >= simultaniousDownloads)
                {
                    var finishedTask = await Task.WhenAny(Tasks);
                    Tasks.Remove(finishedTask);
                    RunQueue<BlobJob>.CleanupTaskList(Tasks);
                }
                Tasks.Add(Task.Run(item.DoJob));
            }

            await Task.WhenAll(Tasks);
            RunQueue<BlobJob>.CleanupTaskList(Tasks);
        }

        internal enum JobType
        {
            None = 0,
            New = 1,
            Modified = 2,
        }

        public class BlobJob
        {
            internal readonly BlobItem Blob;
            internal readonly string LocalFilePath;
            internal ILocalFileInfo FileInfo;
            internal JobType NeedsJob = JobType.None;

            public BlobJob(BlobItem blob, string localFilePath)
            {
                Blob = blob;
                LocalFilePath = localFilePath;
            }

            private static bool WellKnownBlob(BlobItem blob)
            {
                // Ignore empty files
                if (blob.Size == 0)
                    return true;

                // Ignore files only containing "[]"
                if (blob.Size == 2 && blob.MD5 == "11FxOYiYfpMxmANj4kGJzg==")
                    return true;

                return false;
            }

            public async Task<bool> DoJob()
            {
                try
                {
                    LocalFileInfoDisk lfi = null;
                    if (NeedsJob == JobType.New)
                    {
                        Console.Write("N");
                        Directory.CreateDirectory(Path.GetDirectoryName(LocalFilePath));
                    }
                    else if (NeedsJob == JobType.Modified)
                    {
                        lfi = new LocalFileInfoDisk(LocalFilePath);
                        if (FileInfo.Size == Blob.Size &&
                            (FileInfo.MD5 != null && !string.IsNullOrEmpty(Blob.MD5) && FileInfo.MD5 == Blob.MD5))
                        {
                            // since size and hash is the same as last, we just ignore this and don't update
                            if (lfi.Exists && lfi.LastWriteTimeUtc != Blob.LastModifiedUtc.UtcDateTime)
                                File.SetLastWriteTimeUtc(LocalFilePath, Blob.LastModifiedUtc.UtcDateTime);
                            return true;
                        }
                        Console.Write("m");
                        if (lfi.Exists)
                            File.Move(LocalFilePath, LocalFilePath + FLAG_MODIFIED + Blob.LastModifiedUtc.ToString(FLAG_DATEFORMAT) + FLAG_END);
                    }
                    else
                    {
                        return true;
                    }

                    if (WellKnownBlob(Blob))
                    {
                        // no real download of these files
                        NeedsJob = JobType.None;
                        return true;
                    }
                    await Blob.DownloadToFileAsync(LocalFilePath, FileMode.Create);
                    if (lfi == null) lfi = new LocalFileInfoDisk(LocalFilePath);
                    if (lfi.Exists && lfi.LastWriteTimeUtc != Blob.LastModifiedUtc.UtcDateTime)
                        File.SetLastWriteTimeUtc(LocalFilePath, Blob.LastModifiedUtc.UtcDateTime);

                    NeedsJob = JobType.None;
                    return true;
                }
                catch (StorageException ex)
                {
                    // Swallow 404 exceptions.
                    // This will happen if the file has been deleted in the temporary period from listing blobs and downloading
                    Console.Write("Swallowed Ex: " + LocalFilePath + " " + ex.GetType().Name + " " + ex.Message);
                }
                catch (System.IO.IOException ex)
                {
                    Console.Write("Swallowed Ex: " + LocalFilePath + " " + ex.GetType().Name + " " + ex.Message);
                }
                return false;
            }
        }
    }
}
