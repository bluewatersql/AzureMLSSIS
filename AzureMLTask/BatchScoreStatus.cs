using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.AzureML
{
    public class BatchScoreStatus
    {
        public BatchScoreStatusCode StatusCode { get; set; }

        public IDictionary<string, AzureBlobDataReference> Results { get; set; }

        public string Details { get; set; }
    }
}
