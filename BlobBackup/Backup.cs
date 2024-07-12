﻿using Azure;

namespace BlobBackup
{
    internal class Backup : IDisposable
    {
        private readonly string _localPath;
        private readonly string _containerName;

        public ItemCountSize TotalItems = new();
        public ItemCountSize IgnoredItems = new();
        public ItemCountSize UpToDateItems = new();
        public ItemCountSize NewItems = new();
        public ItemCountSize ModifiedItems = new();
        public ItemCountSize DownloadedItems = new();
        public int ExceptionCount = 0;
        public int FailedDownloads = 0;
        public ItemCountSize LocalItems = new();
        public ItemCountSize DeletedItems = new();

        private readonly object ExpectedLocalFilesLock = new();
        private HashSet<string> ExpectedLocalFiles = [];
        private RunQueue<BlobJob> BlobJobQueue = new();
        private readonly object _tasksListLock = new();
        private readonly List<Task> _tasks = [];
        public int TaskCount => _tasks.Count;

        private static readonly HashSet<char> JobChars = [];
        private static readonly object JobCharsLock = new();
        private static DateTime LastConsoleWrite = DateTime.MinValue;
        private static DateTime LastConsoleWriteLine = DateTime.MinValue;
        private static DateTime LastConsoleWriteStats = DateTime.MinValue;

        private FileInfoSqlite _sqlLite;

        public Backup(string localPath, string containerName)
        {
            _localPath = localPath;
            _containerName = containerName;
            _sqlLite = new FileInfoSqlite(containerName, Path.GetFullPath(Path.Combine(_localPath, "..", "sqllite")));
        }

        private const string FLAG_MODIFIED = "[MODIFIED ";
        private const string FLAG_DELETED = "[DELETED ";
        private const string FLAG_DATEFORMAT = "yyyyMMddHHmm";
        private const string FLAG_END = "]";

        internal void AddTasks(params Task[] tasks)
        {
            lock (_tasksListLock)
                _tasks.AddRange(tasks);
        }

        private Task[] GetTasks()
        {
            lock (_tasksListLock)
                return [.. _tasks];
        }

        internal async Task WaitTaskAndClean()
        {
            var taskSet = GetTasks();
            if (taskSet.Length == 0)
                return;
            var finishedTask = await Task.WhenAny(taskSet);
            lock (_tasksListLock)
            {
                _tasks.Remove(finishedTask);
                RunQueue<BlobJob>.CleanupTaskList(_tasks);
            }
        }

        private static ParallelQuery<FileInfo> EnumerateFilesParallel(DirectoryInfo dir) =>
            dir.EnumerateDirectories()
                .SelectMany(EnumerateFilesParallel)
                .Concat(dir.EnumerateFiles("*", SearchOption.TopDirectoryOnly))
                .AsParallel();

        private bool DoLocalFileDelete(FileInfo f)
        {
            if (f.Name.Contains(FLAG_MODIFIED) || f.Name.Contains(FLAG_DELETED))
                return false;
            if (LocalItems.Add(f.Length).count % 1000 == 0)
            {
                CheckPrintConsole();
            }

            var localFilename = f.FullName; // container is needed as well
            if (localFilename.StartsWith(_localPath))
                localFilename = localFilename.Substring(_localPath.Length + 1);
            return !ExpectedLocalFiles.Contains(localFilename);
    }

        private void AddDownloaded(long size) => DownloadedItems.Add(size);

        private BlobJob GetBlobJob((long size, BlobItem blob) sizeblob)
        {
            var itemCount = TotalItems.Add(sizeblob.size);
            if (itemCount.count % 5000 == 0)
            {
                // set progress JobChar for next console update
                AddJobChar('.');
                CheckPrintConsole();
            }

            var blob = sizeblob.blob;
            if (blob is null)
            {
                IgnoredItems.Add(sizeblob.size);
                return null;
            }

            var localFileName = blob.GetLocalFileName();
            var bJob = new BlobJob(this, blob, new(Path.Combine(_localPath, localFileName)));
            if (localFileName is null)
                throw new NullReferenceException();
            lock (ExpectedLocalFilesLock)
                ExpectedLocalFiles.Add(localFileName);

            bJob.FileInfo = _sqlLite.GetFileInfo(blob, bJob.LocalFile);
            bJob.AddDownloaded = AddDownloaded;

            return bJob;
        }

        public async Task<Backup> PrepareJobAsync(string accountName, string accountKey)
        {
            await Task.Yield();
            var localContainerPath = Path.Combine(_localPath, _containerName);
            Directory.CreateDirectory(localContainerPath);
            var localDir = new DirectoryInfo(localContainerPath);

            bool? downloadOk = null;
            try
            {
                await foreach (var blobBatch in BlobItem.BlobEnumeratorAsync(_containerName, accountName, accountKey, GetBlobJob))
                {
                    _sqlLite.BeginTransaction(); // reduce number of writes to sql file
                    var didWorkAny = false;
                    blobBatch.Where(j => j is not null).ForAll(bJob =>
                    {
                        var blob = bJob.Blob;
                        var file = bJob.FileInfo;
                        try
                        {
                            if (!file.Exists || bJob.ForceDownloadMissing())
                            {
                                NewItems.Add(blob.Size);
                                bJob.NeedsJob = JobType.New;
                                bJob.SqlFileInfo.UpdateFromAzure(blob);

                                if (bJob.HandleWellKnownBlob())
                                    IgnoredItems.Add(blob.Size);
                                else
                                    BlobJobQueue.AddDone(bJob);
                            }
                            else if (!file.IsSame(blob))
                            {
                                bJob.NeedsJob = JobType.Modified;
                                BlobJobQueue.AddDone(bJob);
                                ModifiedItems.Add(blob.Size);
                                didWorkAny = true;
                            }
                            else
                            {
                                UpToDateItems.Add(blob.Size);
                            }
                        }
                        catch (Exception ex)
                        {
                            downloadOk = false;
                            Interlocked.Increment(ref ExceptionCount);
                            Console.WriteLine($"INSIDE LOOP EXCEPTION while scanning {_containerName}. Item: {blob.Name} Scanned: {TotalItems}. Ex message:{ex.Message}");
                        }
                    });
                    downloadOk ??= true;
                    if (didWorkAny) 
                        _sqlLite.EndTransaction();
                }
            }
            catch (Exception ex)
            {
                downloadOk = false;
                Interlocked.Increment(ref ExceptionCount);
                Console.WriteLine($"OUTER EXCEPTION ({_containerName}) #{TotalItems}: {ex.Message}");
            }
            finally
            {
                _sqlLite.EndTransaction();
                CheckPrintConsole(true);
                Console.WriteLine(" Fetch done");
            }

            var nowUtc = DateTime.UtcNow;
            var delTask = Task.Run(() =>
            {
                if (!downloadOk.HasValue || !downloadOk.Value)
                {
                    Console.WriteLine(" Due to exception, no delete check will be done");
                    return;
                }
                Console.WriteLine(" Starting delete files known in local sql but not in azure");
                _sqlLite.GetAllFileInfos().AsParallel().
                    Where(fi => !ExpectedLocalFiles.Contains(fi.LocalName)).
                    ForAll(fileInfo =>
                {
                    AddJobChar('d');
                    fileInfo.DeleteDetectedTime = nowUtc;
                    fileInfo.UpdateDb();
                    string fileName = Path.Combine(_localPath, fileInfo.LocalName);
                    var fi = new FileInfo(fileName);

                    var newName = fileName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END;
                    if (fi.Exists)
                        fi.MoveTo(newName);
                    else if (Directory.Exists(Path.GetDirectoryName(newName)))
                        File.Create(newName + ".empty").Close(); // creates dummy file to mark as deleted
                    DeletedItems.Add(fi.Exists ? 0 : fi.Length);
                });
                CheckPrintConsole(true);
                Console.WriteLine(" Delete files known in local sql but not in azure done");
                Console.WriteLine(" Starting delete existing local files not in azure");

                // scan for deleted files by checking if we have a file locally that we did not find remotely
                // load list of local files
                // this might take a minute or 2 if many files, since we wait for first yielded item before continuing
                // done after sql Loop, since that should "remove" them already
                EnumerateFilesParallel(localDir).
                    Where(DoLocalFileDelete).
                    ForAll(fi =>
                {
                    AddJobChar('D');
                    fi.MoveTo(fi.FullName + FLAG_DELETED + nowUtc.ToString(FLAG_DATEFORMAT) + FLAG_END);
                    DeletedItems.Add(fi.Length);
                });
                CheckPrintConsole(true);
                Console.WriteLine(" Delete existing local files not in azure done");
            });
            AddTasks(delTask);
            BlobJobQueue.RunnerDone();

            return this;
        }

        internal static bool AddJobChar(char j)
        {
            lock (JobCharsLock)
            {
                return JobChars.Add(j);
            }
        }

        internal bool CheckPrintConsole(bool forceFull = false)
        {
            var utcNow = DateTime.UtcNow;

            char[] jChars = [];
            if (JobChars.Count != 0)
                lock (JobCharsLock)
                {
                    jChars = [.. JobChars];
                    JobChars.Clear();
                }

            if (jChars.Length > 0 && LastConsoleWrite < utcNow.AddSeconds(-10))
            {
                // don't spam console to much, here we print the last Job item we dealt with
                LastConsoleWrite = utcNow;
                Console.Write(string.Join(string.Empty, jChars));
            }

            if (forceFull || LastConsoleWriteLine < utcNow.AddMinutes(-0.5))
            {
                LastConsoleWriteLine = utcNow;
                Console.WriteLine($"\n --MARK-- {utcNow:yyyy-MM-dd HH:mm:ss.ffff} - Currently {TotalItems} scanned, {TaskCount.Format()} tasks, {BlobJobQueue.QueueCount.Format()} waiting jobs");
                if (forceFull || LastConsoleWriteStats < utcNow.AddMinutes(-2))
                {
                    LastConsoleWriteStats = utcNow;
                    PrintStats();
                }
                Console.Out.Flush();

                return true;
            }

            return jChars.Length > 0;
        }

        public void PrintStats()
        {
            Console.WriteLine($" {TotalItems} remote items scanned and found:");
            Console.WriteLine($" {NewItems} new");
            Console.WriteLine($" {ModifiedItems} modified");
            Console.WriteLine($" {DownloadedItems} downloaded");
            Console.WriteLine($" {UpToDateItems} up to date");
            Console.WriteLine($" {IgnoredItems} ignored, {FailedDownloads.Format()} failed, {ExceptionCount.Format()} exceptions");
            Console.WriteLine($" {LocalItems} local");
            Console.WriteLine($" {DeletedItems} local files deleted (or moved)");
        }

        public async Task ProcessJob(int simultaniousDownloads)
        {
            try
            {
                foreach (var item in BlobJobQueue.GetDoneEnumerable())
                {
                    if (TaskCount >= simultaniousDownloads)
                    {
                        CheckPrintConsole();
                        await WaitTaskAndClean();
                    }

                    AddTasks(Task.Run(item.DoJob));
                }

                CheckPrintConsole(true);

                while (TaskCount > 0)
                {
                    await Task.WhenAll(GetTasks());
                    await WaitTaskAndClean();
                }
            }
            finally
            {
                _sqlLite.Dispose();
                _sqlLite = null;
                CheckPrintConsole(true);
            }
        }

        internal enum JobType
        {
            None = 0,
            New = 1,
            Modified = 2,
        }

        public class BlobJob(Backup bakParent, BlobItem blob, FileInfo localFile)
        {
            internal readonly Backup Bak = bakParent;
            internal readonly BlobItem Blob = blob;
            internal readonly FileInfo LocalFile = localFile;
            internal ILocalFileInfo FileInfo;
            internal Action<long> AddDownloaded;
            internal FileInfoSqlite.FileInfo SqlFileInfo => FileInfo as FileInfoSqlite.FileInfo;
            internal JobType NeedsJob = JobType.None;

            private static readonly HashSet<string> HasCreatedDirectories = [];
            private static readonly object _hasCreatedDirectoriesLock = new();

            private static void EnsureDirExists(FileInfo file)
            {
                var dir = file.Directory;
                if (HasCreatedDirectories.Contains(dir.FullName))
                    return;
                lock (_hasCreatedDirectoriesLock)
                {
                    dir.Create();
                    HasCreatedDirectories.Add(dir.FullName);
                }
            }

            private static readonly DateTime ForceExistsFrom = DateTime.Now.AddDays(-30);
            public bool ForceDownloadMissing()
            {
                if (FileInfo.Size != Blob.Size || FileInfo.MD5 != Blob.MD5)
                    return false; // if file changed, expect it to be downloaded by modification instad

                var lfi = SqlFileInfo?.SrcFileInfo;
                if (lfi is not LocalFileInfoDisk)
                    return false;

                // if newer than a month, and no file exits, force it
                if (Blob.LastModifiedTimeUtc > ForceExistsFrom &&
                    !lfi.Exists && !WellKnownBlob(Blob))
                {
                    Console.WriteLine($"\n** Force Download expected existing file {Blob}");
                    return true;
                }

                return false;
            }

            public static bool WellKnownBlob(ILocalFileInfo blob) =>
                (blob.Size, blob.MD5) switch
                {
                    { Size: 0, MD5: "1B2M2Y8AsgTpgAmY7PhCfg==" } => true, // md5 same as 0 byte
                    { Size: 0 } => throw new Exception($"Non known zero size {blob.MD5}"),
                    { Size: < 1024 * 1024, MD5: "1B2M2Y8AsgTpgAmY7PhCfg==" } => true, // md5 same as 0 byte
                    { Size: 2, MD5: "11FxOYiYfpMxmANj4kGJzg==" } => true, // Ignore files only containing "[]"
                    { Size: 2, MD5: "mZFLkyvTelC5g8XnyQrpOw==" } => true, // Ignore files only containing "{}"
                    { Size: 4, MD5: "OZ/GZwhxR0zXzgRYQB/SmQ==" } => true, // Ignore files only containing "[\"\"]"
                    _ => false,
                };

            public bool HandleWellKnownBlob()
            {
                if (!WellKnownBlob(Blob))
                    return false;
                // no real download of these files
                SqlFileInfo.UpdateFromAzure(Blob);
                SqlFileInfo.LastDownloadedTime = DateTime.UtcNow;
                SqlFileInfo.UpdateDb();
                NeedsJob = JobType.None;
                return true;
            }

            public async ValueTask<bool> DoJob()
            {
                try
                {
                    LocalFileInfoDisk lfi = null;
                    if (NeedsJob == JobType.New)
                    {
                        AddJobChar('N');
                    }
                    else if (NeedsJob == JobType.Modified)
                    {
                        AddJobChar('m');
                        lfi = new LocalFileInfoDisk(LocalFile);
                        var noDownloadNeeded =
                            FileInfo.Size == Blob.Size &&
                            FileInfo.MD5 == Blob.MD5 &&
                            (!lfi.Exists || (lfi.Size == Blob.Size && lfi.GetMd5() == Blob.MD5));

                        if (noDownloadNeeded)
                        {
                            // since size and hash is the same as last, we just fix local modification time and update database
                            if (lfi.Exists)
                            {
                                lfi.UpdateWriteTime(Blob.LastModifiedTimeUtc);
                                SqlFileInfo.UpdateFromFileInfo(lfi);
                            }
                            else
                            {
                                SqlFileInfo.UpdateFromAzure(Blob);
                            }
                            SqlFileInfo.UpdateDb();
                            return true;
                        }

                        if (lfi.Exists)
                        {
                            try
                            {
                                if (lfi.Size <= 0  // just remove empty files, shouldn't exist
                                    || lfi.LastModifiedTimeUtc > DateTime.UtcNow.AddHours(-24)) // recently modified files is also just replaced
                                {
                                    lfi.FileInfo.Delete();
                                }
                                else
                                {
                                    var dst = LocalFile.FullName + FLAG_MODIFIED + Blob.LastModifiedUtc.ToString(FLAG_DATEFORMAT) + FLAG_END;
                                    if (File.Exists(dst))
                                    {
                                        File.Delete(dst);
                                    }

                                    File.Move(lfi.FileInfo.FullName, dst);
                                    lfi.FileInfo.Refresh();
                                }
                            }
                            catch (IOException)
                            {
                                Interlocked.Increment(ref Bak.ExceptionCount);
                                // ignore
                            }
                        }
                    }
                    else
                    {
                        return true;
                    }

                    if (HandleWellKnownBlob())
                        return true;

                    EnsureDirExists(LocalFile);
                    SqlFileInfo.UpdateFromAzure(Blob);
                    await Blob.DownloadToFileAsync(LocalFile);
                    SqlFileInfo.LastDownloadedTime = DateTime.UtcNow;
                    AddDownloaded(Blob.Size);
                    // we always want a new file item after download
                    lfi = new LocalFileInfoDisk(LocalFile);
                    lfi.UpdateWriteTime(Blob.LastModifiedTimeUtc);

                    // maybe something changed from orignal data
                    if (lfi.Size != Blob.Size || lfi.GetMd5() != Blob.MD5)
                    {
                        Interlocked.Increment(ref Bak.FailedDownloads);
                        Console.WriteLine($"\n** Download missmatch {lfi.DiffString(Blob)} for {LocalFile} (changed during run?)");
                        return false; // something went bad, we can try on next run if db isn't updated
                    }

                    SqlFileInfo.UpdateFromFileInfo(lfi);
                    SqlFileInfo.DeleteDetectedTime = null;
                    SqlFileInfo.UpdateDb();

                    NeedsJob = JobType.None;
                    return true;
                }
                catch (RequestFailedException ex)
                {
                    Interlocked.Increment(ref Bak.ExceptionCount);
                    // Swallow 404 exceptions.
                    // This will happen if the file has been deleted in the temporary period from listing blobs and downloading
                    Console.WriteLine($"\nSwallowed Ex: {LocalFile} {ex}");
                }
                catch (IOException ex)
                {
                    HasCreatedDirectories.Clear();
                    Interlocked.Increment(ref Bak.ExceptionCount);
                    Console.WriteLine($"\nSwallowed Ex: {LocalFile} {ex}");
                }
                return false;
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    var sqlInstance = _sqlLite;
                    if (sqlInstance is not null)
                    {
                        sqlInstance.Dispose();
                        _sqlLite = null;
                    }

                    var runQ = BlobJobQueue;
                    if (runQ is not null)
                    {
                        runQ.Dispose();
                        BlobJobQueue = null;
                    }
                }
                ExpectedLocalFiles = null;

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
