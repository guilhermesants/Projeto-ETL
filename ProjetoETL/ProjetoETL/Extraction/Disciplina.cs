using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ProjetoETL.Extraction
{
    class Disciplina : Extraction
    {
        public Disciplina(string connectionstringoperacional, string connectionstringdimensional) : base(connectionstringoperacional, connectionstringdimensional)
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
                        command.CommandText = "SELECT cod_disc, nome_disc, carga_horaria FROM DISCIPLINAS";
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
                        command.CommandText = "INSERT INTO DMDISCIPLINAS (cod_disc, nome_disc, carga_horaria) VALUES (@cod_disc, @nome_disc, @carga_horaria)";

                        foreach (DataRow row in dataTable.Rows)
                        {
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@cod_disc", row["cod_disc"]);
                            command.Parameters.AddWithValue("@nome_disc", row["nome_disc"]);
                            command.Parameters.AddWithValue("@carga_horaria", row["carga_horaria"]);

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
