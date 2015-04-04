using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BluewaterSQL.DTS.AzureML
{
    public class BatchScoreRequest
    {
        public AzureBlobDataReference Input { get; set; }
        public IDictionary<string, string> GlobalParameters { get; set; }
    }
}
