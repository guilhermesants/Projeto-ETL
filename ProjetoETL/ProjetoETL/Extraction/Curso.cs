using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ProjetoETL.Extraction
{
    class Curso : Extraction
    {
        public Curso(string connectionstringoperacional, string connectionstringdimensional) : base(connectionstringoperacional, connectionstringdimensional)
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
                        command.CommandText = "SELECT cod_curso, nom_curso, cod_dpto FROM cursos";
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
                        command.CommandText = "INSERT INTO DMCURSOS (cod_curso, nom_curso, cod_dpto) VALUES (@cod_curso, @nom_curso, @cod_dpto)";

                        foreach (DataRow row in dataTable.Rows)
                        {
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@cod_curso", row["cod_curso"]);
                            command.Parameters.AddWithValue("@nom_curso", row["nom_curso"]);
                            command.Parameters.AddWithValue("@cod_dpto", row["cod_dpto"]);

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
