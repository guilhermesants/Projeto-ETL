using ProjetoETL.Extraction;
using System;

namespace ProjetoETL
{
    class Program
    {
        static void Main(string[] args)
        {

            string stringConnectionOperacional = @"Data Source=.\SQLEXPRESS;Initial Catalog=DataBaseLabsBanco;Integrated Security=True";
            string stringConnectionDimensional = @"Data Source=.\SQLEXPRESS;Initial Catalog=DataBaseDimensionalLabsBanco;Integrated Security=True";

            Alunos aluno = new Alunos(stringConnectionOperacional, stringConnectionDimensional);
            aluno.DataExtraction();

            Departamento departamento = new Departamento(stringConnectionOperacional, stringConnectionDimensional);
            departamento.DataExtraction();

            Curso curso = new Curso(stringConnectionOperacional, stringConnectionDimensional);
            curso.DataExtraction();

            Disciplina disciplina = new Disciplina(stringConnectionOperacional, stringConnectionDimensional);
            disciplina.DataExtraction();

            Tempo tempo = new Tempo(stringConnectionOperacional, stringConnectionDimensional);
            tempo.DataExtraction();

            FTAprovados aprovacoes = new FTAprovados(stringConnectionOperacional, stringConnectionDimensional);
            aprovacoes.DataExtraction();
        }
    }
}
