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

        public Backup(string localPath)
        {
            _localPath = localPath;
        }

        private string GetLocalFileName(string localPath, Uri uri)
        {
            var fileName = uri.AbsolutePath.Replace("//", "/").Replace(@"/", @"\").Replace(":", "--COLON--").Substring(1);
            return Path.Combine(localPath, fileName);
        }

        public BackupJob PrepareJob(string containerName, string accountName, string accountKey, IProgress<int> progress)
        {
            var localContainerPath = Path.Combine(_localPath, containerName);
            Directory.CreateDirectory(localContainerPath);

            var job = new BackupJob();

            try
            {
                foreach (var blob in BlobItem.BlobEnumerator(containerName, accountName, accountKey))
                {
                    job.ScannedItems++;
                    try
                    {
                        if (job.ScannedItems % 50000 == 0)
                        {
                            progress.Report(job.ScannedItems);
                        }

                        if (blob == null)
                        {
                            job.IgnoredItems++;
                            continue;
                        }

                        var bJob = new BlobJob(blob, GetLocalFileName(_localPath, blob.Uri));
                        job.AllRemoteFiles.Add(bJob.LocalFileName);

                        var file = new FileInfo(bJob.LocalFileName);

                        if (file.Exists)
                        {
                            if (file.LastWriteTime < blob.LastModified || file.Length != blob.Size)
                            {
                                bJob.NeedsJob = JobType.Modified;
                                job.AllJobs.Add(bJob);
                                job.ModifiedFiles.Add(bJob);
                            }
                            else
                            {
                                job.UpToDateItems++;
                            }
                        }
                        else
                        {
                            bJob.NeedsJob = JobType.New;
                            job.AllJobs.Add(bJob);
                            job.NewFiles.Add(bJob);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"INSIDE LOOP EXCEPTION while scanning {containerName}. Item: {blob.Uri} Scanned Items: #{job.ScannedItems}. Ex message:" + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"OUTER EXCEPTION ({containerName}) #{job.ScannedItems}: " + ex.Message);
            }

            job.Tasks.Add(Task.Run(() =>
            {
                // scan for deleted files by checking if we have a file in the local file system that we did not find remotely
                foreach (var fileName in Directory.GetFiles(localContainerPath, "*", SearchOption.AllDirectories))
                {
                    if (!fileName.Contains("[MODIFIED ") && !fileName.Contains("[DELETED "))
                        continue;
                    if (!job.AllRemoteFiles.Contains(fileName))
                    {
                        Console.Write("D");
                        File.Move(fileName, fileName + $"[DELETED {DateTime.Now.ToString("yyyyMMddHmm")}]");
                        job.DeletedFiles.Add(fileName);
                    }
                }
            }));

            return job;
        }

        public async Task ProcessJob(BackupJob job, int simultaniousDownloads)
        {
            var throttler = new SemaphoreSlim(initialCount: simultaniousDownloads);

            if (job.AllJobs.Any())
            {
                Console.Write($"Working on files {job.AllJobs.Count}: ");
                void releaseThrottler() => throttler.Release();

                foreach (var item in job.AllJobs)
                {
                    item.JobFinally = releaseThrottler;
                    await throttler.WaitAsync();
                    job.Tasks.Add(Task.Run(item.DoJob));
                }
            }

            await Task.WhenAll(job.Tasks);
            Console.WriteLine();
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

        public class BackupJob
        {
            public int ScannedItems = 0;
            public int IgnoredItems = 0;
            public int UpToDateItems = 0;
            public readonly List<BlobJob> NewFiles = new List<BlobJob>();
            public readonly List<BlobJob> ModifiedFiles = new List<BlobJob>();
            public readonly List<string> DeletedFiles = new List<string>();
            public long NewFilesSize => NewFiles.Sum(b => b.Blob.Size);
            public long ModifiedFilesSize => ModifiedFiles.Sum(b => b.Blob.Size);

            internal HashSet<string> AllRemoteFiles = new HashSet<string>();
            internal List<BlobJob> AllJobs = new List<BlobJob>();
            internal List<Task> Tasks = new List<Task>();
        }
    }
}
