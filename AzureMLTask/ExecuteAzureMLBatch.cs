using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;

namespace BluewaterSQL.DTS.AzureML
{
    [DtsTask(DisplayName = "Execute Azure ML Batch", 
        IconResource = "BluewaterSQL.DTS.AzureML.Task.ico",
        UITypeName = "BluewaterSQL.DTS.AzureML.ExecuteAzureMLBatchUI, BluewaterSQL.DTS.AzureML, Version=1.0.0.0, Culture=Neutral, PublicKeyToken=ea43bb68f6bbda18")]
    public class ExecuteAzureMLBatch : Task //, IDTSPersist
    {
        #region Members
        private const string AZURE_ML_BLOB_NAME_FORMAT = "MLDATA_{0}_{1}.csv";

        private bool fireAgain = false;

        private AzureBlobConnectionInfo _azureBlobConnectionInfo = new AzureBlobConnectionInfo();
        private string _azureMLBaseURL;
        private string _azureMLAPIKey;
        private string _source;
        private string _destination;
        private string _connection;
        private string _blobName;
        private int _mlBatchTimeout =  120 * 1000;
        private SourceType _sourceType = SourceType.BlobPath;
        private DestinationType _outputDestination = DestinationType.None;

        private IDTSComponentEvents _componentEvents;
        private Connections _connections;
        private VariableDispenser _variableDispenser;
        #endregion

        #region Properties
        public int MLBatchTimeout
        {
            get { return _mlBatchTimeout; }
            set { _mlBatchTimeout = value; }
        }
        public string AzureMLBaseURL
        {
            get { return _azureMLBaseURL;  }
            set { _azureMLBaseURL = value; }
        }

        public string AzureMLAPIKey
        {
            get { return _azureMLAPIKey; }
            set { _azureMLAPIKey = value; }
        }

        public string BlobName
        {
            get { return _blobName; }
            set { _blobName = value; }
        }

        public SourceType InputSource
        {
            get { return _sourceType;  }
            set { _sourceType = value; }
        }

        public DestinationType OutputDestination
        {
            get { return _outputDestination; }
            set { _outputDestination = value; }
        }

        public string Connection
        {
            get
            {
                return this.GetConnectionName(_connections, _connection);
            }
            set
            {
                _connection = this.GetConnectionID(_connections, value);
            }
        }

        public string Source
        {
            get
            {
                if (this.InputSource == SourceType.FileConnection)
                    return this.GetConnectionName(_connections, _source);

                return _source;
            }
            set
            {
                if (this.InputSource == SourceType.FileConnection)
                    _source = this.GetConnectionID(_connections, value);
                else
                    _source = value;
            }
        }

        public string Destination
        {
            get
            {
                if (this.OutputDestination == DestinationType.FileConnection)
                    return this.GetConnectionName(_connections, _destination);

                return _destination;
            }
            set
            {
                if (this.OutputDestination == DestinationType.FileConnection)
                    _destination = this.GetConnectionID(_connections, value);
                else
                    _destination = value;
            }
        }
        #endregion

        #region Component Validation
        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log)
        {
            try
            {
                //Azure Storage Connection
                if (_connection == null || string.IsNullOrEmpty(_connection.Trim()))
                {
                    componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Connection manager required.", null, 0);
                    return DTSExecResult.Failure;
                }

                if (validateConnection(connections, _connection) == (DtsObject)null)
                {
                    componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Invalid connection manager name.", null, 0);
                    return DTSExecResult.Failure;
                }

                //Source
                if (_source == null || string.IsNullOrEmpty(_source.Trim()))
                {
                    componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Source required", null, 0);
                    return DTSExecResult.Failure;
                }

                if (_sourceType == SourceType.FileConnection)
                {
                    var sourceConn = validateConnection(connections, _source);

                    if (sourceConn == null)
                    {
                        componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Invalid source connection manager name.", null, 0);
                        return DTSExecResult.Failure;
                    }
                }
                else if (_sourceType == SourceType.Variable)
                {
                    if (!isValidVariable(variableDispenser, _source, TypeCode.String))
                    {
                        componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Source variable must be of string data type", null, 0);
                        return DTSExecResult.Failure;
                    }
                }

                //Destination
                if (_outputDestination != DestinationType.None)
                {
                    if (_destination == null || string.IsNullOrEmpty(_destination.Trim()))
                    {
                        componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Destination required", null, 0);
                        return DTSExecResult.Failure;
                    }

                    if (_outputDestination == DestinationType.FileConnection)
                    {
                        var destConn = validateConnection(connections, _destination);

                        if (destConn == null)
                        {
                            componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Invalid destination connection manager name.", null, 0);
                            return DTSExecResult.Failure;
                        }
                    }
                    else if (_outputDestination == DestinationType.Variable)
                    {
                        if (!isValidVariable(variableDispenser, _destination, TypeCode.String))
                        {
                            componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Destination variable must be of string data type", null, 0);
                            return DTSExecResult.Failure;
                        }
                    }
                }

                //Azure ML Config
                if (_azureMLBaseURL == null || string.IsNullOrEmpty(_azureMLBaseURL.Trim()))
                {
                    componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Azure ML base url required.", null, 0);
                    return DTSExecResult.Failure;
                }

                if (_azureMLAPIKey == null || string.IsNullOrEmpty(_azureMLAPIKey.Trim()))
                {
                    componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Azure ML api key required.", null, 0);
                    return DTSExecResult.Failure;
                }

                if (_mlBatchTimeout == 0)
                {
                    _mlBatchTimeout = 300;
                }

                return DTSExecResult.Success;
            }
            catch (Exception)
            {
                return DTSExecResult.Failure;
            }
        }

        private ConnectionManager validateConnection(Connections connections, string conn)
        {
            ConnectionManager connectionManager = (ConnectionManager)null;

            if (connections != null)
            {
                try
                {
                    connectionManager = connections[(object)conn];
                }
                catch
                {
                    return (ConnectionManager)null;
                }
            }
            return connectionManager;
        }

        private bool isValidVariable(VariableDispenser variableDispenser, string var, TypeCode type)
        {
            Variables variables = (Variables)null;

            try
            {
                variableDispenser.LockOneForRead(var, ref variables);

                Variable variable = variables[var];

                if (variable.DataType != type)
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }
            finally
            {
                if (variables != null)
                    variables.Unlock();
            }

            return true;
        }
        #endregion

        #region Execute
        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction)
        {
            _componentEvents = componentEvents;
            _variableDispenser = variableDispenser;
            _connections = connections;

            _azureBlobConnectionInfo = (AzureBlobConnectionInfo)_connections[_connection].AcquireConnection(null);

            Uri uri = (Uri)null;

            if (this.InputSource != SourceType.BlobPath)
            {
                //If a blob name is specified we create one
                if (string.IsNullOrEmpty(_blobName))
                {
                    _blobName = string.Format(AZURE_ML_BLOB_NAME_FORMAT,
                        DateTime.Now.ToString("yyyyMMdd"),
                        Guid.NewGuid());
                }

                uri = UploadFile();

                if (uri == null)
                    return DTSExecResult.Failure;
            }
            else
            {
                uri = new Uri(_source);
            }

            var task = ProcessBatch(uri);
            task.Wait();

            if (task.Result != BatchScoreStatusCode.Finished)
            {
                return DTSExecResult.Failure;
            }

            return DTSExecResult.Success;
        }
        #endregion

        #region Process Batch
        private async System.Threading.Tasks.Task<BatchScoreStatusCode> ProcessBatch(Uri blobUri)
        {
            using (HttpClient client = new HttpClient())
            {
                BatchScoreRequest request = new BatchScoreRequest()
                {
                    Input = new AzureBlobDataReference()
                    {
                        ConnectionString = _azureBlobConnectionInfo.ConnectionString,
                        RelativeLocation = blobUri.LocalPath
                    },
                    GlobalParameters = new Dictionary<string, string>(){}
                };

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", this.AzureMLAPIKey);
                
                var response = await client.PostAsJsonAsync(this.AzureMLBaseURL, request);
                string jobId = await response.Content.ReadAsAsync<string>();

                string jobLocation = string.Format("{0}/{1}?api-version=2.0", this.AzureMLBaseURL, jobId);

                Stopwatch watch = Stopwatch.StartNew();

                bool running = true;
                long lastTick = 0;
                var jobStatus = BatchScoreStatusCode.NotStarted;

                while (running)
                {
                    response = await client.GetAsync(jobLocation);
                    BatchScoreStatus status = await response.Content.ReadAsAsync<BatchScoreStatus>();
                    
                    //Time the job out
                    if (watch.ElapsedMilliseconds > this.MLBatchTimeout)
                    {
                        await client.DeleteAsync(jobLocation);

                        running = false;
                        _componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Job Timed Out", null, 0);
                    }
                    else
                    {
                        jobStatus = status.StatusCode;

                        switch (jobStatus)
                        {
                            case BatchScoreStatusCode.NotStarted:
                            case BatchScoreStatusCode.Running:
                                break;
                            case BatchScoreStatusCode.Failed:
                            case BatchScoreStatusCode.Cancelled:
                                _componentEvents.FireError(-1, "ExecuteAzureMLBatch", status.Details, null, 0);

                                running = false;
                                break;
                            case BatchScoreStatusCode.Finished:
                                running = false;

                                _componentEvents.FireInformation(0, "ExecuteAzureMLBatch",
                                string.Format("Batch {0} finished in {1} seconds", jobId, watch.ElapsedMilliseconds/1000), null, 0, ref fireAgain);

                                if (status.Results.Count > 0)
                                {
                                    var output = status.Results.First();

                                    //Download the results
                                    DownloadResults(output.Key, output.Value);
                                }
                                break;
                        }
                    }

                    if (running)
                    {
                        //Fire information event every 5 seconds
                        if (watch.ElapsedMilliseconds - lastTick > 5000)
                        {
                            lastTick = watch.ElapsedMilliseconds;
                            _componentEvents.FireInformation(0, "ExecuteAzureMLBatch",
                                string.Format("Batch {0} is running with status {1}", jobId, status.StatusCode), null, 0, ref fireAgain);
                        }

                        Thread.Sleep(1000);
                    }
                }

                return jobStatus;
            }
        }
        #endregion

        #region Download Results
        private void DownloadResults(string key, AzureBlobDataReference blobLocation)
        {
            if (_outputDestination != DestinationType.None)
            {
                var credentials = new StorageCredentials(blobLocation.SasBlobToken);
                var cloudBlob = new CloudBlockBlob(new Uri(new Uri(blobLocation.BaseLocation), blobLocation.RelativeLocation), credentials);

                if (_outputDestination == DestinationType.FileConnection)
                {
                    ConnectionManager connectionManager = _connections[_destination];
                    var connection = (object)null;

                    try
                    {
                        connection = connectionManager.AcquireConnection(null);

                        string filePath = connection.ToString();

                        cloudBlob.DownloadToFile(filePath, FileMode.Create);
                    }
                    finally
                    {
                        if (connection != null && connectionManager != null)
                            connectionManager.ReleaseConnection(null);
                    }
                }
                else if (_outputDestination == DestinationType.Variable)
                {
                    var variables = (Variables)null;
                    _variableDispenser.LockOneForWrite(_destination, ref variables);

                    try
                    {
                        var variable = variables[_destination];

                        if (variable != null)
                        {
                            variable.Value = cloudBlob.DownloadText();
                        }
                    }
                    finally
                    {
                        if (variables != null)
                            variables.Unlock();
                    }
                }

                _componentEvents.FireInformation(0, "ExecuteAzureMLBatch",
                    string.Format("The result '{0}' is available: BaseLocation: '{1}' RelativeLocation: '{2}' SasBlobToken: '{3}'",
                        key,
                        blobLocation.BaseLocation,
                        blobLocation.RelativeLocation,
                        blobLocation.SasBlobToken),
                    null, 0, ref fireAgain);
            }
        }
        #endregion

        #region Upload File
        private Uri UploadFile()
        {
            _componentEvents.FireInformation(0, "ExecuteAzureMLBatch", "Uploading data to Azure Blob", null, 0, ref fireAgain);
            
            var blobClient = _azureBlobConnectionInfo.StorageAccount.CreateCloudBlobClient();

            var container = blobClient.GetContainerReference(_azureBlobConnectionInfo.Path);
            container.CreateIfNotExists();

            var blob = container.GetBlockBlobReference(this.BlobName);
            blob.DeleteIfExists();

            if (this.InputSource == SourceType.FileConnection)
            {
                ConnectionManager connectionManager = _connections[_source];
                var connection = (object)null;

                try
                {
                    connection = connectionManager.AcquireConnection(null);

                    string filePath = connection.ToString();

                    if (!System.IO.File.Exists(filePath))
                    {
                        _componentEvents.FireError(-1, "ExecuteAzureMLBatch", "Source File does not exist.", null, 0);
                        return null;
                    }

                    //Upload the file from filepath
                    blob.UploadFromFile(filePath, FileMode.Open);
                }
                finally
                {
                    if (connection != null && connectionManager != null)
                        connectionManager.ReleaseConnection(null);
                }
            } 
            else if (this.InputSource == SourceType.DirectInput)
            {
                blob.UploadText(_source);
            }
            else if (this.InputSource == SourceType.Variable)
            {
                var variables = (Variables)null;
                _variableDispenser.LockOneForRead(_source, ref variables);

                try
                {
                    var variable = variables[_source];

                    if (variable != null)
                    {
                        blob.UploadText(variable.Value.ToString());
                    }
                }
                finally
                {
                    if (variables != null)
                        variables.Unlock();
                }
            }

            return blob.Uri;
        }
        #endregion
    }
}
