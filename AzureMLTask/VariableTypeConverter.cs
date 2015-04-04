using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BluewaterSQL.DTS.AzureML
{
    public class VariableTypeConverter : StringConverter
    {
        private object GetSpecializedObject(object contextInstance)
        {
            return contextInstance;
        }

        public override TypeConverter.StandardValuesCollection GetStandardValues(ITypeDescriptorContext context)
        {
            return new TypeConverter.StandardValuesCollection((ICollection)this.getConnections(this.GetSpecializedObject(context.Instance)));
        }

        public override bool GetStandardValuesExclusive(ITypeDescriptorContext context)
        {
            return true;
        }

        public override bool GetStandardValuesSupported(ITypeDescriptorContext context)
        {
            return true;
        }

        private ArrayList getConnections(object retrievalObject)
        {
            ExecuteAzureMLBatchGeneralView.GeneralViewNode ddlNode = (ExecuteAzureMLBatchGeneralView.GeneralViewNode)retrievalObject;
            ddlNode.iDtsConnection.GetConnections();

            ArrayList arrayList = new ArrayList();
            arrayList.Add("New Variable");

            foreach (Variable variable in ddlNode.myTaskHost.Variables)
            {
                if (!variable.SystemVariable && !variable.ReadOnly && variable.DataType == TypeCode.String)
                    arrayList.Add((object)variable.QualifiedName);
            }

            if (arrayList != null && arrayList.Count > 0)
                arrayList.Sort();

            return arrayList;
        }
    }
}
