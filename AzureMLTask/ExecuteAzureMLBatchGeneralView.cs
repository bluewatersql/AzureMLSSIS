using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.AzureML
{
    public class ExecuteAzureMLBatchGeneralView : UserControl, IDTSTaskUIView
    {
        private PropertyGrid propertyGrid;
        private GeneralViewNode generalNode;

        public ExecuteAzureMLBatchGeneralView()
        {
            InitializeComponent();
        }

        #region IDTSTaskUIView Members
        public void OnCommit(object taskHost)
        {
            TaskHost host = taskHost as TaskHost;
            if (host == null)
            {
                throw new ArgumentException("Argument is not a TaskHost.", "taskHost");
            }

            ExecuteAzureMLBatch task = host.InnerObject as ExecuteAzureMLBatch;
            if (task == null)
            {
                throw new ArgumentException("Argument is not a ExecuteAzureMLBatch task.", "taskHost");
            }

            host.Name = generalNode.Name;
            host.Description = generalNode.Description;

            task.Connection = generalNode.Connection;
            task.AzureMLBaseURL = generalNode.AzureMLUrl;
            task.AzureMLAPIKey = generalNode.AzureMLKey;
            task.BlobName = generalNode.BlobName;
            task.Source = generalNode.Source;
            task.InputSource = generalNode.InputSource;
            task.OutputDestination = generalNode.OutputDestination;
            
            switch (task.InputSource)
            {
                case (SourceType.BlobPath):
                    task.Source = generalNode.SourceBlobPath;
                    break;
                case (SourceType.DirectInput):
                    task.Source = generalNode.SourceDirect;
                    break;
                case (SourceType.FileConnection):
                    task.Source = generalNode.Source;
                    break;
                case (SourceType.Variable):
                    task.Source = generalNode.SourceVariable;
                    break;
                default:
                    break;
            }

            switch (task.OutputDestination)
                {
                    case (DestinationType.FileConnection):
                        task.Destination = generalNode.Destination;
                        break;
                    case (DestinationType.Variable):
                        task.Destination = generalNode.DestinationVariable;
                        break;
                    default:
                        break;
                }
        }

        public void OnInitialize(IDTSTaskUIHost treeHost, TreeNode viewNode, object taskHost, object connections)
        {
            this.generalNode = new GeneralViewNode(taskHost as TaskHost, connections as IDtsConnectionService);
            this.propertyGrid.SelectedObject = generalNode;
        }

        public void OnLoseSelection(ref bool bCanLeaveView, ref string reason)
        {
        }

        public void OnSelection()
        {
        }

        public void OnValidate(ref bool bViewIsValid, ref string reason)
        {        
        }
        #endregion

        #region GeneralNode
        [SortProperties(new string[] { "Connection", "AzureMLBaseURL", "AzureMLAPIKey", "BlobName", 
            "InputSource", "Source", "SourceDirect", "OutputDestination", "Destination" })]
        internal class GeneralViewNode : ICustomTypeDescriptor
        {
            // Properties variables
            private string name = string.Empty;
            private string description = string.Empty;

            private string connection = string.Empty;
            private string azureMLUrl = string.Empty;
            private string azureMLKey = string.Empty;
            private string blobName = string.Empty;
            private SourceType sourceType = SourceType.BlobPath;
            private string source = string.Empty;
            private string sourceDirect = string.Empty;
            private string sourceVariable = string.Empty;
            private string sourceBlobPath = string.Empty;
            private DestinationType destinationType = DestinationType.None;
            private string destination = string.Empty;
            private string destinationVariable = string.Empty;

            internal IDtsConnectionService iDtsConnection;
            internal TaskHost myTaskHost;
            private IDtsVariableService _variableService;

            internal IDtsVariableService VariableService
            {
                get
                {
                    return this._variableService;
                }
            }

            internal TaskHost DtsTaskHost
            {
                get
                {
                    return this.myTaskHost;
                }
            }

            internal GeneralViewNode(TaskHost taskHost, IDtsConnectionService connectionService)
            {
                this.iDtsConnection = connectionService;
                this.myTaskHost = taskHost;
                this._variableService = this.myTaskHost.Site.GetService(typeof (IDtsVariableService)) as IDtsVariableService;

                // Extract common values from the Task Host
                name = taskHost.Name;
                description = taskHost.Description;

                // Extract values from the task object
                ExecuteAzureMLBatch task = taskHost.InnerObject as ExecuteAzureMLBatch;
                if (task == null)
                {
                    string msg = string.Format(CultureInfo.CurrentCulture, "Type mismatch for taskHost inner object. Received: {0} Expected: {1}", taskHost.InnerObject.GetType().Name, typeof(ExecuteAzureMLBatch).Name);
                    throw new ArgumentException(msg);
                }

                connection = task.Connection;
                azureMLUrl = task.AzureMLBaseURL;
                azureMLKey = task.AzureMLAPIKey;
                blobName = task.BlobName;
                sourceType = task.InputSource;                
                destinationType = task.OutputDestination;

                switch (sourceType)
                {
                    case (SourceType.BlobPath):
                        sourceBlobPath = task.Source;
                        break;
                    case (SourceType.DirectInput):
                        sourceDirect = task.Source;
                        break;
                    case (SourceType.FileConnection):
                        source = task.Source;
                        break;
                    case (SourceType.Variable):
                        sourceVariable = task.Source;
                        break;
                    default:
                        break;
                }

                switch (destinationType)
                {
                    case (DestinationType.FileConnection):
                        destination = task.Destination;
                        break;
                    case (DestinationType.Variable):
                        destinationVariable = task.Destination;
                        break;
                    default:
                        break;
                }
            }

            #region Properties
            [Category("General"), Description("Task name")]            
            public string Name
            {
                get
                {
                    return name;
                }
                set
                {
                    string v = value.Trim();
                    if (string.IsNullOrEmpty(v))
                    {
                        throw new ArgumentException("Task name cannot be empty");
                    }
                    name = v;
                }
            }

            [Category("General"), Description("Task description")]
            public string Description
            {
                get
                {
                    return description;
                }
                set
                {
                    description = value;
                }
            }

            [Category("General"), Description("Azure Storage Connection Manager")]
            [TypeConverter(typeof(ConnectionTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string Connection
            {
                get { return connection; }
                set { connection = value; }
            }

            [Category("Azure ML Web Service"), Description("Web Service API URL Endpoint")]
            public string AzureMLUrl
            {
                get { return azureMLUrl; }
                set { azureMLUrl = value; }
            }

            [Category("Azure ML Web Service"), Description("Web Service API Secret Key")]
            public string AzureMLKey
            {
                get { return azureMLKey; }
                set { azureMLKey = value; }
            }

            [Category("General"), Description("Optional blob name used when writing input file to Azure Blob Storage")]
            public string BlobName
            {
                get { return blobName; }
                set { blobName = value; }
            }

            [Category("Source"), Description("Source Type")]
            [RefreshProperties(RefreshProperties.All)]
            public SourceType InputSource
            {
                get { return sourceType; }
                set { sourceType = value; }
            }

            [Category("Source"), Description("Source File Connection")]
            [TypeConverter(typeof(FileConnectionTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string Source
            {
                get { return source; }
                set { source = value; }
            }

            [Category("Source"), Description("Source Direct Input")]
            public string SourceDirect
            {
                get { return sourceDirect; }
                set { sourceDirect = value; }
            }

            [Category("Source"), Description("Source Variable")]
            [TypeConverter(typeof(VariableTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string SourceVariable
            {
                get { return sourceVariable; }
                set { sourceVariable = value; }
            }

            [Category("Source"), Description("Source Blob Url")]
            public string SourceBlobPath
            {
                get { return sourceBlobPath; }
                set { sourceBlobPath = value; }
            }

            [Category("Destination"), Description("Destination Type")]
            [RefreshProperties(RefreshProperties.All)]
            public DestinationType OutputDestination
            {
                get { return destinationType; }
                set { destinationType = value; }
            }

            [Category("Destination"), Description("Destination File Connection")]
            [TypeConverter(typeof(FileConnectionTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string Destination
            {
                get { return destination; }
                set { destination = value;  }
            }

            [Category("Destination"), Description("Destination Variable")]
            [TypeConverter(typeof(VariableTypeConverter))]
            [RefreshProperties(RefreshProperties.All)]
            public string DestinationVariable
            {
                get { return destinationVariable; }
                set { destinationVariable = value; }
            }
            #endregion

            #region Interface Methods
            public PropertyDescriptorCollection GetProperties(Attribute[] attributes)
            {
                Attribute[] attributeArray = new Attribute[1];
                attributeArray[0] = (Attribute)new BrowsableAttribute(false);

                PropertyDescriptorCollection properties1 = TypeDescriptor.GetProperties((object)this, attributes, true);
                PropertyDescriptor[] propertyDescriptorArray = new PropertyDescriptor[properties1.Count];
                PropertyDescriptor[] properties2 = new PropertyDescriptor[properties1.Count];
                properties1.CopyTo((Array)propertyDescriptorArray, 0);
                properties1.CopyTo((Array)properties2, 0);

                Hashtable hashtable = new Hashtable();
                for (int index = 0; index < propertyDescriptorArray.Length; ++index)
                    hashtable.Add((object)propertyDescriptorArray[index].Name, (object)index);

                switch (sourceType)
                {
                    case (SourceType.BlobPath):
                        properties2[(int)hashtable["Source"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["Source"], attributeArray);
                        properties2[(int)hashtable["SourceDirect"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceDirect"], attributeArray);
                        properties2[(int)hashtable["SourceVariable"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceVariable"], attributeArray);
                        break;
                    case (SourceType.DirectInput):
                        properties2[(int)hashtable["Source"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["Source"], attributeArray);
                        properties2[(int)hashtable["SourceBlobPath"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceBlobPath"], attributeArray);
                        properties2[(int)hashtable["SourceVariable"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceVariable"], attributeArray);
                        break;
                    case (SourceType.FileConnection):
                        properties2[(int)hashtable["SourceBlobPath"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceBlobPath"], attributeArray);
                        properties2[(int)hashtable["SourceDirect"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceDirect"], attributeArray);
                        properties2[(int)hashtable["SourceVariable"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceVariable"], attributeArray);
                        break;
                    case (SourceType.Variable):
                        properties2[(int)hashtable["Source"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["Source"], attributeArray);
                        properties2[(int)hashtable["SourceDirect"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceDirect"], attributeArray);
                        properties2[(int)hashtable["SourceBlobPath"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["SourceBlobPath"], attributeArray);
                        break;
                    default:
                        break;
                }

                switch (destinationType)
                {
                    case (DestinationType.None):
                        properties2[(int)hashtable["Destination"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["Destination"], attributeArray);
                        properties2[(int)hashtable["DestinationVariable"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["DestinationVariable"], attributeArray);
                        break;
                    case (DestinationType.FileConnection):
                        properties2[(int)hashtable["DestinationVariable"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["DestinationVariable"], attributeArray);
                        break;
                    case (DestinationType.Variable):
                        properties2[(int)hashtable["Destination"]] = TypeDescriptor.CreateProperty(typeof(ExecuteAzureMLBatchGeneralView.GeneralViewNode), properties1["Destination"], attributeArray);
                        break;
                    default:
                        break;
                }

                return new PropertyDescriptorCollection(properties2);
            }

            public TypeConverter GetConverter()
            {
                return TypeDescriptor.GetConverter((object)this, true);
            }

            public EventDescriptorCollection GetEvents(Attribute[] attributes)
            {
                return TypeDescriptor.GetEvents((object)this, attributes, true);
            }

            public EventDescriptorCollection GetEvents()
            {
                return TypeDescriptor.GetEvents((object)this, true);
            }

            public string GetComponentName()
            {
                return TypeDescriptor.GetComponentName((object)this, true);
            }

            public object GetPropertyOwner(PropertyDescriptor pd)
            {
                return (object)this;
            }

            public AttributeCollection GetAttributes()
            {
                return TypeDescriptor.GetAttributes((object)this, true);
            }

            public PropertyDescriptorCollection GetProperties()
            {
                return this.GetProperties(new Attribute[0]);
            }

            public object GetEditor(System.Type editorBaseType)
            {
                return TypeDescriptor.GetEditor((object)this, editorBaseType, true);
            }

            public PropertyDescriptor GetDefaultProperty()
            {
                return TypeDescriptor.GetDefaultProperty((object)this, true);
            }

            public EventDescriptor GetDefaultEvent()
            {
                return TypeDescriptor.GetDefaultEvent((object)this, true);
            }

            public string GetClassName()
            {
                return TypeDescriptor.GetClassName((object)this, true);
            }
            #endregion
        }
        #endregion

        #region Designer code
        private void InitializeComponent()
        {
            this.propertyGrid = new System.Windows.Forms.PropertyGrid();
            this.SuspendLayout();
            // 
            // propertyGrid
            // 
            this.propertyGrid.Dock = System.Windows.Forms.DockStyle.Fill;
            this.propertyGrid.Location = new System.Drawing.Point(0, 0);
            this.propertyGrid.Name = "propertyGrid";
            this.propertyGrid.PropertySort = System.Windows.Forms.PropertySort.Categorized;
            this.propertyGrid.Size = new System.Drawing.Size(150, 150);
            this.propertyGrid.TabIndex = 0;
            this.propertyGrid.ToolbarVisible = false;
            this.propertyGrid.PropertyValueChanged += propertyGrid_PropertyValueChanged;

            // 
            // GeneralView
            // 
            this.Controls.Add(this.propertyGrid);
            this.Name = "GeneralView";
            this.ResumeLayout(false);
        }

        private void propertyGrid_PropertyValueChanged(object s, PropertyValueChangedEventArgs e)
        {
            if (e.ChangedItem.PropertyDescriptor.Name.Equals("SourceVariable"))
            {
                if (e.ChangedItem.Value.Equals("New Variable"))
                {
                    this.generalNode.SourceVariable = null;
                    this.generalNode.SourceVariable = PromptForVariable(e);
                } 
            }
            else if (e.ChangedItem.PropertyDescriptor.Name.Equals("Source"))
            {
                if (e.ChangedItem.Value.Equals("New Connection"))
                {
                    this.generalNode.Source = null;
                    this.generalNode.Source = PromptForConnection(e, "FILE");
                }
            }
            else if (e.ChangedItem.PropertyDescriptor.Name.Equals("DestinationVariable"))
            {
                if (e.ChangedItem.Value.Equals("New Variable"))
                {
                    this.generalNode.DestinationVariable = null;
                    this.generalNode.DestinationVariable = PromptForVariable(e);
                }                
            }
            else if (e.ChangedItem.PropertyDescriptor.Name.Equals("Destination"))
            {
                if (e.ChangedItem.Value.Equals("New Connection"))
                {
                    this.generalNode.Destination = null;
                    this.generalNode.Destination = PromptForConnection(e, "FILE");
                }
            }
            else if (e.ChangedItem.PropertyDescriptor.Name.Equals("Connection"))
            {
                if (e.ChangedItem.Value.Equals("New Connection"))
                {
                    this.generalNode.Connection = null;
                    this.generalNode.Connection = PromptForConnection(e, "AZURESTORAGE");
                }
            }
        }

        private string PromptForConnection(PropertyValueChangedEventArgs e, string connectionType)
        {
            this.Cursor = Cursors.WaitCursor;
            ArrayList destination = this.generalNode.iDtsConnection.CreateConnection(connectionType);
            this.Cursor = Cursors.Default;

            if (destination != null && destination.Count > 0)
                return ((Microsoft.SqlServer.Dts.Runtime.ConnectionManager)destination[0]).Name;
            else if (e.OldValue == null)
                return null;
            else
                return (string)e.OldValue;
        }

        private string PromptForVariable(PropertyValueChangedEventArgs e)
        {
            this.Cursor = Cursors.WaitCursor;
            IDtsVariableService variableService = this.generalNode.VariableService;
            this.Cursor = Cursors.Default;

            if (variableService != null)
            {
                Variable variable = variableService.PromptAndCreateVariable((IWin32Window)this, (DtsContainer)this.generalNode.DtsTaskHost);

                if ((DtsObject)variable == null)
                {
                    return (string)e.OldValue;
                }
                else if (!variable.ReadOnly && variable.DataType == TypeCode.String)
                {
                    return variable.QualifiedName;
                }
                else
                {
                    return (string)e.OldValue;
                }
            }

            return (string)e.OldValue;
        }
        #endregion
    }
}
