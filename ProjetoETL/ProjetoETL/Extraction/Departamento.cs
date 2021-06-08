using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ProjetoETL.Extraction
{
    public class Departamento : Extraction
    {
        public Departamento(string connectionstringoperacional, string connectionstringdimensional) : base(connectionstringoperacional, connectionstringdimensional)
        {
        }

        public override void DataExtraction()
        {
            DataTable dataTable = new DataTable();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionStringOperacionalDataBase))
                {
                    connection.Open();

                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "SELECT cod_dpto, nome_dpto FROM departamentos";
                        command.ExecuteNonQuery();

                        var adapter = new SqlDataAdapter(command);
                        adapter.Fill(dataTable);
                    }

                    connection.Close();
                }

                using (SqlConnection connection = new SqlConnection(connectionStringDimensionalDataBase))
                {
                    connection.Open();

                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "INSERT INTO DMDEPARTAMENTOS (cod_dpto, nome_dpto) VALUES (@cod_dpto, @nome_dpto)";

                        foreach (DataRow row in dataTable.Rows)
                        {
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@cod_dpto", row["cod_dpto"]);
                            command.Parameters.AddWithValue("@nome_dpto", row["nome_dpto"]);

                            command.ExecuteNonQuery();
                        }
                    }
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


        }
    }
    
}
