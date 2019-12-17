﻿using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobBackup
{
    public class FileInfoSqlite : IDisposable
    {
        private readonly string _sqlLitePath;
        private readonly object dbConnectionLock = new object();
        private SQLiteConnection dbConnection;

        private const string SQL_TABLENAME = "files";

        public FileInfoSqlite(string containerName, string sqlLitePath)
        {
            _sqlLitePath = sqlLitePath;

            Directory.CreateDirectory(_sqlLitePath);
            var sqlFile = Path.Combine(_sqlLitePath, containerName + ".sqlite");
            var sqlFileMissing = !File.Exists(sqlFile);
            if (sqlFileMissing) SQLiteConnection.CreateFile(sqlFile);

            dbConnection = new SQLiteConnection($"Data Source={sqlFile};Version=3;");
            dbConnection.Open();

            // note that column types are specified for clearity rather than what SQLite uses internally
            ExecuteNonQuery(
                "CREATE TABLE IF NOT EXISTS " + SQL_TABLENAME + " (" +
                " LocalName VARCHAR(1024) NOT NULL PRIMARY KEY," +
                " RemPath VARCHAR(1024) NOT NULL," +
                " LastModifiedTime DATETIME NOT NULL," +
                " Size INT NOT NULL," +
                " AzureMD5 VARCHAR(32) NOT NULL," +
                " LastDownloadedTime DATETIME NULL," +
                " DeleteDetectedTime DATETIME NULL" +
                ");");
        }

        private SQLiteCommand GetCmd(string sql, object[] parameters)
        {
            var cmd = new SQLiteCommand(sql, dbConnection);
            if (parameters != null)
            {
                int paramNumber = 1;
                foreach (object value in parameters)
                {
                    cmd.Parameters.AddWithValue(paramNumber.ToString(), value);
                    paramNumber++;
                }
            }

            return cmd;
        }

        /// <remarks>This uses locks internally so should be safe to call</remarks>
        private int ExecuteNonQuery(string sql, params object[] parameters)
        {
            lock (dbConnectionLock)
            {
                using (var cmd = GetCmd(sql, parameters))
                    return cmd.ExecuteNonQuery();
            }
        }

        /// <remarks>MUST be locked from outside</remarks>
        private SQLiteDataReader ExecuteReader(string sql, params object[] parameters)
        {
            using (var cmd = GetCmd(sql, parameters))
                return cmd.ExecuteReader();
        }

        private static DateTime? GetDateTime(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;
            return Convert.ToDateTime(value);
        }

        private const string SQL_BEGINTRANSACTION = "BEGIN TRANSACTION";
        private const string SQL_ENDTRANSACTION = "END TRANSACTION";

        private bool inTransaction;

        public bool BeginTransaction()
        {
            if (inTransaction)
                return false;
            ExecuteNonQuery(SQL_BEGINTRANSACTION);
            inTransaction = true;
            return true;
        }

        public bool EndTransaction()
        {
            if (!inTransaction)
                return false;
            ExecuteNonQuery(SQL_ENDTRANSACTION);
            inTransaction = false;
            return true;
        }

        internal FileInfo GetFileInfo(ILocalFileInfo lfi, string LocalName = null)
        {
            lock (dbConnectionLock)
            {
                using (var reader = ExecuteReader("SELECT * FROM " + SQL_TABLENAME + " WHERE LocalName=@1", LocalName))
                {
                    if (reader.Read())
                    {
                        return new FileInfo(this, lfi, reader);
                    }
                }
            }
            return null;
        }

        internal IEnumerable<FileInfo> GetAllFileInfos()
        {
            lock (dbConnectionLock)
            {
                using (var reader = ExecuteReader("SELECT * FROM " + SQL_TABLENAME + " WHERE DeleteDetectedTime IS NULL"))
                {
                    while (reader.Read())
                    {
                        yield return new FileInfo(this, null, reader);
                    }
                }
            }
        }

        public ILocalFileInfo GetFileInfo(BlobItem blob, string localFilename = null)
        {
            ILocalFileInfo lfi = localFilename == null ? null : new LocalFileInfoDisk(localFilename);
            var fi = GetFileInfo(lfi, blob.GetLocalFileName());
            if (fi != null)
                return fi;

            // create new instance
            fi = new FileInfo(this, lfi, blob);

            // make sure we insert item when there is nothing found
            ExecuteNonQuery("INSERT INTO " + SQL_TABLENAME +
                " (LocalName, RemPath, LastModifiedTime, Size, AzureMD5, LastDownloadedTime, DeleteDetectedTime)" +
                " VALUES (@1, @2, @3, @4, @5, @6, @7)",
                fi.LocalName, fi.RemPath, fi.LastModifiedTime, fi.Size, fi.MD5, fi.LastDownloadedTime, fi.DeleteDetectedTime);

            return fi;
        }

        private void UpdateFileInfo(FileInfo fi)
        {
            ExecuteNonQuery("UPDATE " + SQL_TABLENAME +
                " SET RemPath=@2, LastModifiedTime=@3, Size=@4, AzureMD5=@5, LastDownloadedTime=@6, DeleteDetectedTime=@7" +
                " WHERE LocalName=@1",
                fi.LocalName, fi.RemPath, fi.LastModifiedTime, fi.Size, fi.MD5, fi.LastDownloadedTime, fi.DeleteDetectedTime);
        }

        public class FileInfo : ILocalFileInfo
        {
            private readonly FileInfoSqlite _sqlLite;

            public bool Exists => LastDownloadedTime.HasValue;
            public long Size { get; set; }
            public string MD5 { get; set; }
            public DateTime LastWriteTimeUtc => LastDownloadedTime ?? DateTime.MinValue;

            public string LocalName { get; private set; }
            public string RemPath { get; private set; }
            public DateTime LastModifiedTime { get; private set; }
            public DateTime? LastDownloadedTime { get; set; }
            public DateTime? DeleteDetectedTime { get; set; }

            private FileInfo(FileInfoSqlite sqlite)
            {
                _sqlLite = sqlite;
            }

            internal FileInfo(FileInfoSqlite sqlite, ILocalFileInfo fi, SQLiteDataReader reader)
                : this(sqlite)
            {
                LocalName = Convert.ToString(reader["LocalName"]);
                RemPath = Convert.ToString(reader["RemPath"]);
                LastModifiedTime = Convert.ToDateTime(reader["LastModifiedTime"]);
                Size = Convert.ToInt64(reader["Size"]);
                MD5 = Convert.ToString(reader["AzureMD5"]);
                LastDownloadedTime = GetDateTime(reader["LastDownloadedTime"]);
                DeleteDetectedTime = GetDateTime(reader["DeleteDetectedTime"]);

                // if there is local file, than it takes precedence over database values
                UpdateFromFileInfo(fi);
            }

            private FileInfo(FileInfoSqlite sqlite, BlobItem blob)
                : this(sqlite)
            {
                UpdateFromAzure(blob);
            }

            internal void UpdateFromAzure(BlobItem blob)
            {
                LocalName = blob.GetLocalFileName();
                RemPath = blob.Uri.AbsolutePath;
                LastModifiedTime = blob.LastModifiedUtc.UtcDateTime;
                Size = blob.Size;
                MD5 = blob.MD5;
            }

            internal FileInfo(FileInfoSqlite sqlite, ILocalFileInfo fi, BlobItem blob)
                : this(sqlite, blob)
            {
                UpdateFromFileInfo(fi);
            }

            internal void UpdateFromFileInfo(ILocalFileInfo fi)
            {
                if (fi == null || !fi.Exists)
                    return;
                LastDownloadedTime = fi.LastWriteTimeUtc;
                Size = fi.Size;
                if (!string.IsNullOrEmpty(fi.MD5))
                    MD5 = fi.MD5;
            }

            internal void UpdateDb()
            {
                _sqlLite.UpdateFileInfo(this);
            }

            public override string ToString()
            {
                return string.Join("|", RemPath, LastModifiedTime, Size, MD5, LastDownloadedTime, DeleteDetectedTime);
            }
        }


        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                EndTransaction();
                dbConnection.Close();
                if (disposing)
                {
                    // dispose managed state (managed objects).
                    dbConnection.Dispose();
                }

                // free unmanaged resources (unmanaged objects) and override a finalizer below.
                // set large fields to null.

                disposedValue = true;
            }
        }

        ~FileInfoSqlite()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}