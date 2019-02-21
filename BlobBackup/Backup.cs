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
        private string _localPath;

        public int ScannedItems = 0;
        public int IgnoredItems = 0;
        public int UpToDateItems = 0;
        public readonly List<BlobJob> NewFiles = new List<BlobJob>();
        public readonly List<BlobJob> ModifiedFiles = new List<BlobJob>();
        public readonly List<string> DeletedFiles = new List<string>();
        public long NewFilesSize => NewFiles.Sum(b => b.Blob.Size);
        public long ModifiedFilesSize => ModifiedFiles.Sum(b => b.Blob.Size);

        internal HashSet<string> AllRemoteFiles = new HashSet<string>();
        internal RunQueue<BlobJob> BlobJobQueue = new RunQueue<BlobJob>();
        internal List<Task> Tasks = new List<Task>();

        public Backup(string localPath)
        {
            _localPath = localPath;
        }

        private string GetLocalFileName(string localPath, Uri uri)
        {
            var fileName = uri.AbsolutePath.Replace("//", "/").Replace(@"/", @"\").Replace(":", "--COLON--").Substring(1);
            return Path.Combine(localPath, fileName);
        }

        public Backup PrepareJob(string containerName, string accountName, string accountKey, IProgress<int> progress)
        {
            var localContainerPath = Path.Combine(_localPath, containerName);
            Directory.CreateDirectory(localContainerPath);

            try
            {
                foreach (var blob in BlobItem.BlobEnumerator(containerName, accountName, accountKey))
                {
                    ScannedItems++;
                    try
                    {
                        if (ScannedItems % 50000 == 0)
                        {
                            progress.Report(ScannedItems);
                        }

                        if (blob == null)
                        {
                            IgnoredItems++;
                            continue;
                        }

                        var bJob = new BlobJob(blob, GetLocalFileName(_localPath, blob.Uri));
                        AllRemoteFiles.Add(bJob.LocalFileName);

                        var file = new FileInfo(bJob.LocalFileName);

                        if (file.Exists)
                        {
                            if (file.LastWriteTime < blob.LastModified || file.Length != blob.Size)
                            {
                                bJob.NeedsJob = JobType.Modified;
                                BlobJobQueue.AddDone(bJob);
                                ModifiedFiles.Add(bJob);
                            }
                            else
                            {
                                UpToDateItems++;
                            }
                        }
                        else
                        {
                            bJob.NeedsJob = JobType.New;
                            BlobJobQueue.AddDone(bJob);
                            NewFiles.Add(bJob);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"INSIDE LOOP EXCEPTION while scanning {containerName}. Item: {blob.Uri} Scanned Items: #{ScannedItems}. Ex message:" + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OUTER EXCEPTION ({containerName}) #{ScannedItems}: " + ex.Message);
            }
            BlobJobQueue.RunnerDone();

            Tasks.Add(Task.Run(() =>
            {
                // scan for deleted files by checking if we have a file in the local file system that we did not find remotely
                foreach (var fileName in Directory.GetFiles(localContainerPath, "*", SearchOption.AllDirectories))
                {
                    if (!fileName.Contains("[MODIFIED ") && !fileName.Contains("[DELETED "))
                        continue;
                    if (!AllRemoteFiles.Contains(fileName))
                    {
                        Console.Write("D");
                        File.Move(fileName, fileName + $"[DELETED {DateTime.Now.ToString("yyyyMMddHmm")}]");
                        DeletedFiles.Add(fileName);
                    }
                }
            }));

            return this;
        }

        public async Task ProcessJob(int simultaniousDownloads)
        {
            var throttler = new SemaphoreSlim(initialCount: simultaniousDownloads);
            void releaseThrottler() => throttler.Release();

            foreach (var item in BlobJobQueue.GetDoneEnumerable())
            {
                item.JobFinally = releaseThrottler;
                await throttler.WaitAsync();
                Tasks.Add(Task.Run(item.DoJob));
            }

            await Task.WhenAll(Tasks);
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
            internal readonly string LocalFileName;
            internal JobType NeedsJob = JobType.None;
            internal Action JobFinally;

            public BlobJob(BlobItem blob, string localFileName)
            {
                Blob = blob;
                LocalFileName = localFileName;
            }

            public async Task DoJob()
            {
                try
                {
                    if (NeedsJob == JobType.New)
                    {
                        Console.Write("N");
                        Directory.CreateDirectory(Path.GetDirectoryName(LocalFileName));
                    }
                    else if (NeedsJob == JobType.Modified)
                    {
                        Console.Write("m");
                        File.Move(LocalFileName, LocalFileName + $"[MODIFIED {Blob.LastModified.Value.ToString("yyyyMMddHmm")}]");
                    }
                    else
                    {
                        return;
                    }

                    await Blob.DownloadToFileAsync(LocalFileName, FileMode.Create);
                    NeedsJob = JobType.None;
                }
                catch (StorageException ex)
                {
                    // Swallow 404 exceptions.
                    // This will happen if the file has been deleted in the temporary period from listing blobs and downloading
                    Console.Write("Swallowed Ex: " + ex.Message);
                }
                finally
                {
                    JobFinally();
                }
            }
        }
    }
}
