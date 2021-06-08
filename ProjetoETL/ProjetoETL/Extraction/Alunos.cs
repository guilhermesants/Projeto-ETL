using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ProjetoETL.Extraction
{
    class Alunos : Extraction
    {
        public Alunos(string connectionstringOperacional, string connectionstringDimensional) : base(connectionstringOperacional, connectionstringDimensional)
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
                        command.CommandText = "SELECT mat_alu, nome, dat_entrada, cotista FROM alunos";
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
                        command.CommandText = "INSERT INTO DMALUNOS (mat_alu, nome, data_entrada, cotista) VALUES (@mat_alu, @nome, @data_entrada, @cotista)";

                        foreach (DataRow row in dataTable.Rows)
                        {
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@mat_alu", row["mat_alu"]);
                            command.Parameters.AddWithValue("@nome", row["nome"]);
                            command.Parameters.AddWithValue("@data_entrada", row["dat_entrada"]);
                            command.Parameters.AddWithValue("@cotista", row["cotista"]);
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
