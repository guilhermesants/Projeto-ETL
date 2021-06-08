using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ProjetoETL.Extraction
{
    public class FTAprovados : Extraction
    {
        public FTAprovados(string connectionstringoperacional, string connectionstringdimensional) : base(connectionstringoperacional, connectionstringdimensional)
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
                        command.CommandText = "select distinct alu.mat_alu, cur.cod_curso, dis.cod_disc, mat.nota, mat.status, mat.faltas, mat.semestre, mat_cur.periodo, alu.cotista from alunos alu" +
                                                " join cursos cur on(alu.cod_curso = cur.cod_curso)" +
                                                " join matriculas mat on(alu.mat_alu = mat.mat_alu)" +
                                                " join disciplinas dis on(mat.cod_disc = dis.cod_disc)" +
                                                " left join matrizes_cursos mat_cur on(mat.cod_disc = mat_cur.cod_disc)";
                        command.ExecuteNonQuery();

                        var adapter = new SqlDataAdapter(command);
                        adapter.Fill(dataTable);
                    }

                    connection.Close();
                }

                var dataTableTempo = DMTempo(connectionStringDimensionalDataBase);

                using (SqlConnection connection = new SqlConnection(connectionStringDimensionalDataBase))
                {
                    connection.Open();

                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "INSERT INTO FT_APROVACOES (mat_alu, cod_curso, cod_disc, id_tempo, nota1, status, faltas, cotista) VALUES (@mat_alu, @cod_curso, @cod_disc, @id_tempo, @nota1, @status, @faltas, @cotista)";

                        foreach (DataRow row in dataTable.Rows)
                        {

                            string idtempo = null;
                            int semestreAno = Convert.ToInt32(row["semestre"]);

                             
                            foreach (DataRow drow in dataTableTempo.Rows)
                            {
                                var semestre = drow["semestre"].ToString();
                                var ano = drow["ano"].ToString();

                                var semestreano = semestre + ano;
                                var SemestreAno = Convert.ToInt32(semestreano);

                                if (semestreAno == SemestreAno)
                                {
                                    idtempo = drow["id_tempo"].ToString();
                                    break;
                                }
                                
                            }

                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@mat_alu", row["mat_alu"]);
                            command.Parameters.AddWithValue("@cod_curso", row["cod_curso"]);
                            command.Parameters.AddWithValue("@cod_disc", row["cod_disc"]);
                            command.Parameters.AddWithValue("@id_tempo", idtempo);
                            command.Parameters.AddWithValue("@nota1", row["nota"]);
                            command.Parameters.AddWithValue("@status", row["status"]);
                            command.Parameters.AddWithValue("@faltas", row["faltas"]);
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

        private DataTable DMTempo(string connectionStringDimensional)
        {
            var dataTable = new DataTable();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionStringDimensionalDataBase))
                {
                    connection.Open();

                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "Select * from DMTEMPO";
                        command.ExecuteNonQuery();

                        var adapter = new SqlDataAdapter(command);
                        adapter.Fill(dataTable);
                    }
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return dataTable;
        }
    }
}
