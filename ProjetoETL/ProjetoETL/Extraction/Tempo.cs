using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ProjetoETL.Extraction
{
    public class Tempo : Extraction
    {
        public Tempo(string connectionstringoperacional, string connectionstringdimensional) : base(connectionstringoperacional, connectionstringdimensional)
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
                        command.CommandText = "SELECT distinct mat_alu, semestre, periodo from matriculas mat" +
                                                " join matrizes_cursos matriz on mat.cod_disc = matriz.cod_disc";
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
                        command.CommandText = "INSERT INTO DMTEMPO (id_tempo, ano, semestre, periodo) VALUES (@id_tempo, @ano, @semestre, @periodo)";

                        foreach (DataRow row in dataTable.Rows)
                        {

                            var anosemestre = row["semestre"];
                            var anosemestreToString = anosemestre.ToString();
                            var ano = anosemestreToString.Substring(1, 4);
                            var semestre = anosemestreToString.Substring(0, 1);

                            var id = Guid.NewGuid();
                            var idtempo = id.ToString();

                            var Ano = Convert.ToInt32(ano);
                            var Semestre = Convert.ToInt32(semestre);

                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@id_tempo", idtempo);
                            command.Parameters.AddWithValue("@ano", Ano);
                            command.Parameters.AddWithValue("@semestre", Semestre);
                            command.Parameters.AddWithValue("@periodo", row["periodo"]);

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
