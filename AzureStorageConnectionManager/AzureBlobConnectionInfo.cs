using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.AzureML
{
    public class AzureBlobConnectionInfo
    {
        public CloudStorageAccount StorageAccount { get; set; }

        public string ConnectionString { get; set; }
        public string Separator { get; set; }
        public string Path { get; set; }
        public bool HasHeaders { get; set; }
        public string FilePattern { get; set; }
        public int FileSize { get; set; }
        public bool IncludeHeaders { get; set; }

    }
}
