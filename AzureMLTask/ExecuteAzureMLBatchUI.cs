using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Design;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace BluewaterSQL.DTS.AzureML
{
    public class ExecuteAzureMLBatchUI : IDtsTaskUI
    {
        private TaskHost _taskHost = null;
        private IDtsConnectionService _connectionService = null;
        private IServiceProvider _serviceProvider = null;

        #region IDtsTaskUI Members
        public void Delete(IWin32Window parentWindow)
        {
        }

        public ContainerControl GetView()
        {
            return new ExecuteAzureMLBatchUIMainWnd(_taskHost, _serviceProvider, _connectionService);
        }

        public void Initialize(TaskHost taskHost, IServiceProvider serviceProvider)
        {
            this._taskHost = taskHost;
            this._serviceProvider = serviceProvider;
            this._connectionService = serviceProvider.GetService(typeof(IDtsConnectionService)) as IDtsConnectionService;
        }

        public void New(IWin32Window parentWindow)
        {
        }
        #endregion
    }
}
