using Microsoft.DataTransformationServices.Controls;
using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BluewaterSQL.DTS.AzureML
{
    public class ExecuteAzureMLBatchUIMainWnd : DTSBaseTaskUI
    {
        private IServiceProvider serviceProvider = null;

        // UI properties
        private const string Title = "Execute Azure ML Batch";
        private const string Description = "Submits a batch job to the Azure Machine Learning Web Service";
        private static Icon TaskIcon = new Icon(typeof(ExecuteAzureMLBatch).Assembly.GetManifestResourceStream("BluewaterSQL.DTS.AzureML.Task.ico"));

        private ExecuteAzureMLBatchGeneralView _generalView;
        public ExecuteAzureMLBatchGeneralView GeneralView
        {
            get { return _generalView; }
        }

        public ExecuteAzureMLBatchUIMainWnd(TaskHost taskHost, IServiceProvider serviceProvider, object connections) :
            base(Title, TaskIcon, Description, taskHost, connections)
        {            
            InitializeComponent();

            this.serviceProvider = serviceProvider;

            // Setup our views
            _generalView = new ExecuteAzureMLBatchGeneralView();

            this.DTSTaskUIHost.FastLoad = false;
            this.DTSTaskUIHost.AddView("General", _generalView, null);
            this.DTSTaskUIHost.FastLoad = true;
        }

        #region Designer code
        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
        }

        #endregion
    }
}
