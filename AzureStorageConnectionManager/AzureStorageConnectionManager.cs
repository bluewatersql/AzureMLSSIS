using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BluewaterSQL.DTS.AzureML
{
    [DtsConnection(ConnectionType = "AZURESTORAGE",
        DisplayName = "Windows Azure Storage",
        Description = "Connection manager for Windows Azure Storage")]
    public class AzureStorageConnectionManager : ConnectionManagerBase
    {
        // Default values.
        private string _storageName = "";
        private string _accessKey = "";
        private bool _useDevelopmentStorage = true;
        private string _connectionString = String.Empty;

        private string _path;
        private string _delimiter = ",";
        private bool _hasHeaders = false;
        private string _filePattern = "";
        private int _fileSize = 0;
        private bool _includeHeaders = false;

        private const string CONNECTIONSTRING_TEMPLATE = "DefaultEndpointsProtocol=https;AccountName=[StorageName];AccountKey=[StorageKey]";
        private const string CONNECTIONSTRING_LOCAL = "UseDevelopmentStorage=true";

        public string StorageName
        {
            get { return _storageName; }
            set { _storageName = value; }
        }

        public string AccessKey
        {
            get { return _accessKey; }
            set { _accessKey = value; }
        }

        public bool UseDevelopmentStorage
        {
            get { return _useDevelopmentStorage; }
            set
            {
                _useDevelopmentStorage = value;
                UpdateConnectionString(value);
            }
        }

        public override string ConnectionString
        {
            get
            {
                UpdateConnectionString(UseDevelopmentStorage);
                return _connectionString;
            }
            set { _connectionString = value; }
        }

        private void UpdateConnectionString(bool isLocalConnection = false)
        {
            if (isLocalConnection)
            {
                _connectionString = CONNECTIONSTRING_LOCAL;
            }
            else
            {
                string temporaryString = CONNECTIONSTRING_TEMPLATE;

                if (!String.IsNullOrEmpty(_storageName))
                {
                    temporaryString = temporaryString.Replace("[StorageName]", _storageName);
                }

                if (!String.IsNullOrEmpty(_accessKey))
                {
                    temporaryString = temporaryString.Replace("[StorageKey]", _accessKey);
                }

                _connectionString = temporaryString;
            }
        }

        public string Path
        {
            get { return _path; }
            set { _path = value; }
        }
        public string Delimiter
        {
            get { return _delimiter; }
            set { _delimiter = value; }
        }
        public bool HasHeaders
        {
            get { return _hasHeaders; }
            set { _hasHeaders = value; }
        }
        public string FilePattern
        {
            get { return _filePattern; }
            set { _filePattern = value; }
        }
        public int FileSize
        {
            get { return _fileSize; }
            set { _fileSize = value; }
        }
        public bool IncludeHeaders
        {
            get { return _includeHeaders; }
            set { _includeHeaders = value; }
        }


        private AzureBlobConnectionInfo _blobStorageInfo;
        
        public override Microsoft.SqlServer.Dts.Runtime.DTSExecResult Validate(Microsoft.SqlServer.Dts.Runtime.IDTSInfoEvents infoEvents)
        {
            if (!_useDevelopmentStorage && (String.IsNullOrEmpty(_storageName) || String.IsNullOrEmpty(_accessKey)))
            {
                infoEvents.FireError(0, "AzureBlobStorageConnectionManager", "No storage name/ access key specified", String.Empty, 0);
                return Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure;
            }

            try
            {
                var storageAccount = CloudStorageAccount.Parse(_connectionString);

                if (storageAccount == null)
                    throw new Exception();
            }
            catch
            {
                infoEvents.FireError(0, "AzureBlobStorageConnectionManager", "Connection to storage account failed", String.Empty, 0);
                return Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure;
            }

            return Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success;
        }

        public override object AcquireConnection(object txn)
        {
            UpdateConnectionString(UseDevelopmentStorage);

            _blobStorageInfo = new AzureBlobConnectionInfo()
            {
                StorageAccount = CloudStorageAccount.Parse(_connectionString),
                ConnectionString = _connectionString,
                Path = this.Path,
                Separator = this.Delimiter,
                HasHeaders = this.HasHeaders,
                FilePattern = this.FilePattern,
                FileSize = this.FileSize,
                IncludeHeaders = this.IncludeHeaders
            };

            return _blobStorageInfo;
        }
    }
}
