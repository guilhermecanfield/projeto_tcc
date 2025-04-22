# scripts/extract_transform.py

import os
import polars as pl
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
    Concatena todos os arquivos Parquet do mesmo tipo em um único arquivo anual.
    """
    print("Concatenando arquivos por tabela...")

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

        if 'tbestabelecimento' in base.lower():
            dfs = [df.drop('ST_COWORKING') if 'ST_COWORKING' in df.columns else df for df in dfs]

        df_final = pl.concat(dfs, how='vertical_relaxed')
        df_final.write_parquet(os.path.join(FINAL_DIR, f'{base}_{ANO}.parquet'))
        print(f'{base}_{ANO}.parquet salvo.')


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


def tratar_estabelecimentos():
    """
    Trata a tabela de estabelecimentos: mantém apenas registros ativos e únicos.
    """
    print("\nTratando tbEstabelecimento...")

    df = ler_arquivo_polars('data/cnes/tbestabelecimento_2022.parquet')

    df = df.select([
        'CO_UNIDADE', 'CO_CNES', 'NU_CNPJ_MANTENEDORA', 'TP_PFPJ',
        'NIVEL_DEP', 'NO_RAZAO_SOCIAL', 'NO_FANTASIA', 'NO_LOGRADOURO',
        'NU_ENDERECO', 'NO_COMPLEMENTO', 'NO_BAIRRO', 'CO_CEP',
        'NU_CNPJ', 'CO_ATIVIDADE', 'TP_UNIDADE', 'CO_TURNO_ATENDIMENTO',
        'CO_ESTADO_GESTOR', 'CO_MUNICIPIO_GESTOR', 'CO_MOTIVO_DESAB',
        'TP_ESTAB_SEMPRE_ABERTO', 'CO_TIPO_UNIDADE', 'CO_TIPO_ESTABELECIMENTO',
        'CO_ATIVIDADE_PRINCIPAL', 'data_competencia'
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