# scripts/extract_transform.py

import os
import polars as pl
import polars.selectors as cs
from scripts.utils import (
    criar_pastas, 
    ler_arquivo_polars,
    adicionar_coluna_data,
    ler_cidades_ibge
)

# Pastas
RAW_DIR = 'data/raw'
PARQUET_DIR = 'data/parquet'
FINAL_DIR = 'data/cnes'

criar_pastas([RAW_DIR, PARQUET_DIR, FINAL_DIR])

# Configurações
ANO = '2022'

# Funções
def transformar_dados():
    """
    Lê os CSVs baixados, ajusta encoding/delimitador e salva em Parquet.
    """
    print("Transformando CSVs em Parquet...")

    arquivos = os.listdir(RAW_DIR)

    for arquivo in arquivos:
        if not arquivo.lower().endswith('.csv'):
            continue

        caminho_csv = os.path.join(RAW_DIR, arquivo)
        nome_base = arquivo.replace('.csv', '').lower()

        print(f"{arquivo} → {nome_base}.parquet")

        df = ler_arquivo_polars(caminho_csv)
        df = adicionar_coluna_data(df, arquivo)
        df.write_parquet(os.path.join(PARQUET_DIR, f"{nome_base}.parquet"))

    print("Conversão para Parquet finalizada.")


def concatenar_parquets_por_tabela():
    """
    Concatena arquivos Parquet por tabela base, alinhando colunas diferentes.
    """
    print("Concatenando arquivos por tabela base...")
    arquivos = os.listdir(PARQUET_DIR)
    tabelas_encontradas = {}

    for arquivo in arquivos:
        if not arquivo.endswith('.parquet'):
            continue
        nome_base = ''.join(filter(str.isalpha, arquivo.replace('.parquet', '')))
        tabelas_encontradas.setdefault(nome_base.lower(), []).append(arquivo)

    for base, arquivos_base in tabelas_encontradas.items():
        caminhos = [os.path.join(PARQUET_DIR, f) for f in arquivos_base]
        print(f'Concatenando {len(caminhos)} arquivos da tabela {base}...')

        dfs = [ler_arquivo_polars(c) for c in caminhos]

        # Alinhar todas as colunas
        colunas_totais = set()
        for df in dfs:
            colunas_totais.update(df.columns)
        colunas_totais = sorted(colunas_totais)  # opcional: manter ordenado

        dfs_alinhados = []
        for df in dfs:
            faltando = list(set(colunas_totais) - set(df.columns))
            if faltando:
                for col in faltando:
                    df = df.with_columns(pl.lit(None).alias(col))
            # reordenar para manter o mesmo padrão
            df = df.select(colunas_totais)
            dfs_alinhados.append(df)

        # 🔹 Finalmente concatena
        df_final = pl.concat(dfs_alinhados, how='vertical_relaxed')

        os.makedirs(FINAL_DIR, exist_ok=True)
        df_final.write_parquet(os.path.join(FINAL_DIR, f'{base}_{ANO}.parquet'))
        print(f'Tabela {base}_{ANO}.parquet salva!')


def tratar_e_deduplicar_tabelas():
    """
    Remove registros duplicados considerando as tabelas que precisam ou não de comparação com dezembro.
    """

    tabelas_sem_mudanca = [
        'tbAtividade', 'tbMunicipio', 'tbEstado',
        'tbTipoUnidade', 'tbTipoEstabelecimento', 'tbAtributo'
    ]

    tabelas_com_mudanca = [
        'rlEstabComplementar', 'tbAtividadeProfissional'
    ]

    def processar_tabelas(lista_nomes, verificar_igual=True):
        for nome_tabela in lista_nomes:
            print(f"\nProcessando {nome_tabela}...")

            caminho_parquet = f'{FINAL_DIR}/{nome_tabela.lower()}_{ANO}.parquet'
            df = ler_arquivo_polars(caminho_parquet)

            if nome_tabela not in tabelas_com_mudanca:
                df = df.sort('data_competencia')

            df_unique = df.unique(subset=df.columns[:-1], keep='last')

            if verificar_igual:
                caminho_dezembro = f'data/parquet/{nome_tabela}{ANO}12.parquet'
                dezembro = ler_arquivo_polars(caminho_dezembro)
                print(f"Antes: {df.shape} | Depois: {df_unique.shape} | Dezembro: {dezembro.shape}")

                if df_unique.height == dezembro.height:
                    print(f'Sem alterações relevantes.')

            caminho_destino = f'{FINAL_DIR}/{nome_tabela.lower()}_{ANO}.parquet'
            df_unique.write_parquet(caminho_destino)
            print(f'{nome_tabela} salvo em {caminho_destino}')

    processar_tabelas(tabelas_sem_mudanca, verificar_igual=True)
    processar_tabelas(tabelas_com_mudanca, verificar_igual=False)

    print("\nTodas tabelas deduplicadas e salvas com sucesso.")


def tratar_estabelecimentos(nome_arquivo: str = 'tbestabelecimento_2022.parquet'):
    """
    Trata a tabela de estabelecimentos: mantém apenas registros ativos e únicos.
    """
    print("\nTratando tbEstabelecimento...")

    df = ler_arquivo_polars(f'data/cnes/{nome_arquivo}')

    df = df.select([
        'CO_UNIDADE', 'CO_CNES', 'NU_CNPJ_MANTENEDORA', 'TP_PFPJ',
        'NIVEL_DEP', 'NO_RAZAO_SOCIAL', 'NO_FANTASIA', 'NO_LOGRADOURO',
        'NU_ENDERECO', 'NO_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP',
        'NU_CNPJ', 'CO_ATIVIDADE', 'TP_UNIDADE', 'CO_TURNO_ATENDIMENTO',
        'CO_ESTADO_GESTOR', 'CO_MUNICIPIO_GESTOR', 'CO_MOTIVO_DESAB',
        'TP_ESTAB_SEMPRE_ABERTO', 'CO_TIPO_UNIDADE', 'CO_TIPO_ESTABELECIMENTO',
        'CO_ATIVIDADE_PRINCIPAL', 'TP_GESTAO', 'data_competencia'
    ])

    estabelecimento_datas = df.group_by('CO_CNES').agg(
        pl.col('data_competencia').first().alias('data_primeiro_registro'),
        pl.col('data_competencia').last().alias('data_ultimo_registro')
    )

    df = df.join(estabelecimento_datas, on='CO_CNES', how='inner')
    df = df.sort('data_ultimo_registro')
    df = df.lazy().unique(subset=["CO_CNES"], keep='last').collect()

    ativos = df.filter(pl.col('CO_MOTIVO_DESAB') == '')
    print(f"Estabelecimentos ativos em 2022: {ativos['CO_CNES'].n_unique()} unidades.")

    df.write_parquet('data/cnes/tbestabelecimento_2022.parquet')

    print("tbEstabelecimento tratado e salvo com sucesso!")

def trata_dados_cidades():
    """
    Lê o CSV de cidades do IBGE, limpa e salva em Parquet.
    """
    criar_pastas(['data/cidades_final'])
    print("Lendo dados de cidades do IBGE...")
    
    dfs = []
    for file in os.listdir('data/cidades'):
        df = ler_cidades_ibge('data/cidades/' + file)
        dfs.append(df)
    
    df = pl.concat(dfs, how='vertical_relaxed')

    print("Salvando dados tratados...")
    
    df.write_parquet('data/cidades_final/cidades_ibge_2022.parquet')
    
    print("Arquivo de cidades tratado e salvo!")

def transformar_dados_mortalidade(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica transformações semânticas aos campos codificados da base de mortalidade.
    """

    df = df.with_columns([
        pl.when(pl.col("SEXO") == "0").then(pl.lit("Ignorado"))
          .when(pl.col("SEXO") == "1").then(pl.lit("Masculino"))
          .when(pl.col("SEXO") == "2").then(pl.lit("Feminino"))
          .otherwise(pl.lit("Desconhecido")).alias("SEXO"),

        pl.when(pl.col("RACACOR") == "1").then(pl.lit("Branca"))
          .when(pl.col("RACACOR") == "2").then(pl.lit("Preta"))
          .when(pl.col("RACACOR") == "3").then(pl.lit("Amarela"))
          .when(pl.col("RACACOR") == "4").then(pl.lit("Parda"))
          .when(pl.col("RACACOR") == "5").then(pl.lit("Indígena"))
          .otherwise(pl.lit("Ignorado")).alias("RACACOR"),

        pl.when(pl.col("ESTCIV") == "1").then(pl.lit("Solteiro"))
          .when(pl.col("ESTCIV") == "2").then(pl.lit("Casado"))
          .when(pl.col("ESTCIV") == "3").then(pl.lit("Viúvo"))
          .when(pl.col("ESTCIV") == "4").then(pl.lit("Separado/Divorciado"))
          .when(pl.col("ESTCIV") == "5").then(pl.lit("União estável"))
          .otherwise(pl.lit("Ignorado")).alias("ESTCIV"),

        pl.when(pl.col("ESC2010") == "0").then(pl.lit("Sem escolaridade"))
          .when(pl.col("ESC2010") == "1").then(pl.lit("Fundamental I"))
          .when(pl.col("ESC2010") == "2").then(pl.lit("Fundamental II"))
          .when(pl.col("ESC2010") == "3").then(pl.lit("Médio"))
          .when(pl.col("ESC2010") == "4").then(pl.lit("Superior incompleto"))
          .when(pl.col("ESC2010") == "5").then(pl.lit("Superior completo"))
          .otherwise(pl.lit("Ignorado")).alias("ESC2010"),

        pl.when(pl.col("LOCOCOR") == "1").then(pl.lit("Hospital"))
          .when(pl.col("LOCOCOR") == "2").then(pl.lit("Outros estabelecimentos de saúde"))
          .when(pl.col("LOCOCOR") == "3").then(pl.lit("Domicílio"))
          .when(pl.col("LOCOCOR") == "4").then(pl.lit("Via pública"))
          .when(pl.col("LOCOCOR") == "5").then(pl.lit("Outros"))
          .when(pl.col("LOCOCOR") == "6").then(pl.lit("Aldeia indígena"))
          .otherwise(pl.lit("Ignorado")).alias("LOCOCOR"),

        pl.when(pl.col("GRAVIDEZ") == "1").then(pl.lit("Única"))
          .when(pl.col("GRAVIDEZ") == "2").then(pl.lit("Dupla"))
          .when(pl.col("GRAVIDEZ") == "3").then(pl.lit("Tripla ou mais"))
          .otherwise(pl.lit("Ignorado")).alias("GRAVIDEZ"),

        pl.when(pl.col("PARTO") == "1").then(pl.lit("Vaginal"))
          .when(pl.col("PARTO") == "2").then(pl.lit("Cesáreo"))
          .otherwise(pl.lit("Ignorado")).alias("PARTO"),

        pl.when(pl.col("OBITOPARTO") == "1").then(pl.lit("Antes"))
          .when(pl.col("OBITOPARTO") == "2").then(pl.lit("Durante"))
          .when(pl.col("OBITOPARTO") == "3").then(pl.lit("Depois"))
          .otherwise(pl.lit("Ignorado")).alias("OBITOPARTO"),

        pl.when(pl.col("TPMORTEOCO") == "1").then(pl.lit("Gravidez"))
          .when(pl.col("TPMORTEOCO") == "2").then(pl.lit("Parto"))
          .when(pl.col("TPMORTEOCO") == "3").then(pl.lit("Abortamento"))
          .when(pl.col("TPMORTEOCO") == "4").then(pl.lit("Até 42 dias pós-parto"))
          .when(pl.col("TPMORTEOCO") == "5").then(pl.lit("43 dias a 1 ano pós-parto"))
          .when(pl.col("TPMORTEOCO") == "8").then(pl.lit("Não ocorreu neste período"))
          .otherwise(pl.lit("Ignorado")).alias("TPMORTEOCO"),

        pl.when(pl.col("CIRCOBITO") == "1").then(pl.lit("Acidente"))
          .when(pl.col("CIRCOBITO") == "2").then(pl.lit("Suicídio"))
          .when(pl.col("CIRCOBITO") == "3").then(pl.lit("Homicídio"))
          .when(pl.col("CIRCOBITO") == "4").then(pl.lit("Outros"))
          .otherwise(pl.lit("Ignorado")).alias("CIRCOBITO"),

        pl.when(pl.col("ACIDTRAB") == "1").then(pl.lit("Sim"))
          .when(pl.col("ACIDTRAB") == "2").then(pl.lit("Não"))
          .otherwise(pl.lit("Ignorado")).alias("ACIDTRAB"),

        pl.when(pl.col("FONTE") == "1").then(pl.lit("Ocorrência policial"))
          .when(pl.col("FONTE") == "2").then(pl.lit("Hospital"))
          .when(pl.col("FONTE") == "3").then(pl.lit("Família"))
          .when(pl.col("FONTE") == "4").then(pl.lit("Outra"))
          .otherwise(pl.lit("Ignorado")).alias("FONTE")
    ])

    df = df.with_columns(cs.string().str.strip_chars().str.to_titlecase())
    return df

def tratar_dados_mortalidade(nome_arquivo: str = 'DOBR2022.parquet'):
    """
    Trata os dados de mortalidade do SUS (SIM), realiza join com municípios e estados e salva em Parquet.
    """
    print("Lendo dados de mortalidade...")

    criar_pastas(['data/mortalidade'])

    df = ler_arquivo_polars(f'data/sim/{nome_arquivo}')

    df = transformar_dados_mortalidade(df)

    # Carrega tabelas de município e estado tratadas
    municipios = ler_arquivo_polars('data/cnes/tbmunicipio_2022.parquet')
    municipios = municipios.with_columns([
        pl.col('CO_MUNICIPIO').cast(pl.Utf8)
    ])
    # estados = ler_arquivo_polars('data/cnes/tbestado_2022.parquet')

    # Join com municípios
    df = df.join(
        municipios.select(['CO_MUNICIPIO', 'NO_MUNICIPIO', 'CO_SIGLA_ESTADO']),
        left_on='CODMUNOCOR',
        right_on='CO_MUNICIPIO',
        how='left'
    )

    # # Join com estados (via município, usando a coluna UF)
    # df = df.join(
    #     estados.select(['uf', 'sigla', 'descricao']),
    #     on='uf',
    #     how='left'
    # )

    df.write_parquet('data/mortalidade/mortalidade_2022.parquet')
    print("Tabela de mortalidade salva com sucesso.")
