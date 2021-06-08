using System;
using System.Collections.Generic;
using System.Text;

namespace ProjetoETL.Extraction
{
    public class Extraction
    {
        public string connectionStringOperacionalDataBase;
        public string connectionStringDimensionalDataBase;

        public Extraction (string connectionstringoperacional, string connectionstringdimensional)
        {
            connectionStringOperacionalDataBase = connectionstringoperacional;
            connectionStringDimensionalDataBase = connectionstringdimensional;
        }

        public virtual void DataExtraction() { }
    }
}
