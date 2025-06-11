import os
import logging
import re
import unicodedata
import polars as pl

def criar_pastas(pastas: list):
    """
    Cria uma lista de pastas, caso não existam.

    Args:
        pastas (list): Lista com os caminhos das pastas a serem criadas.
    """
    for pasta in pastas:
        if not os.path.exists(pasta):
            os.makedirs(pasta, exist_ok=True)
            print(f"Pasta criada: {pasta}")
        else:
            print(f"Pasta já existe: {pasta}")

def configurar_logger(nome_logger="engenharia_de_dados"):
    """
    Configura um logger básico para o projeto.

    Args:
        nome_logger (str): Nome do logger.
    """
    logger = logging.getLogger(nome_logger)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def ler_arquivo_polars(caminho: str) -> pl.DataFrame:
    """
    Lê um arquivo csv, xlsx ou Parquet usando Polars, de forma inteligente baseado na extensão.

    Args:
        caminho (str): Caminho completo do arquivo.

    Returns:
        pl.DataFrame: DataFrame lido.
    """
    extensao = os.path.splitext(caminho)[-1].lower()

    if extensao == '.parquet':
        return pl.read_parquet(caminho)
    elif extensao == '.xlsx':
        return pl.read_excel(caminho)
    elif extensao == '.csv':
        return pl.read_csv(
                caminho, 
                separator=';', 
                encoding='latin1', 
                infer_schema_length=10000,
                low_memory=True,
                schema_overrides={
                    'CO_UNIDADE': pl.Utf8
                }
            )
    else:
        raise ValueError(f"Extensão de arquivo não suportada: {extensao}")
    
def limpar_nome_coluna(coluna: str) -> str:
    """
    Limpa e padroniza nomes de colunas:
    - Remove HTML
    - Remove acentos e cedilha
    - Substitui espaços e hifens por _
    - Remove caracteres especiais
    - Remove múltiplos _
    - Deixa tudo em minúsculo
    """
    # Remove qualquer html tipo <span> ou <br>
    coluna = re.sub(r'<.*?>', '', coluna)

    # Normaliza unicode para remover acentos (acentos, cedilha etc.)
    coluna = unicodedata.normalize('NFKD', coluna)
    coluna = coluna.encode('ASCII', 'ignore').decode('utf-8')

    # Substitui espaços e hífens por underline
    coluna = re.sub(r'[\s\-]+', '_', coluna)

    # Remove tudo que não for letra, número ou underscore
    coluna = re.sub(r'[^a-zA-Z0-9_]', '', coluna)

    # Substitui múltiplos underlines por apenas um
    coluna = re.sub(r'_+', '_', coluna)

    # Remove underlines no início e no fim
    coluna = coluna.strip('_')

    # Converte pra minúsculo
    coluna = coluna.lower()

    return coluna

def ler_cidades_ibge(path_csv):
    """
    Lê o CSV de cidades do IBGE, ignora cabeçalho e rodapé, limpa colunas e retorna um Polars DataFrame.
    """

    # Lê o CSV pulando as 2 primeiras linhas
    df = pl.read_excel(
        path_csv,
        read_options={"skip_rows": 1}
    )

    linhas = [0,1,2,3]

    for linha in linhas:
        if 'Município [-]' in df.row(linha) or 'Municipio [-]' in df.row(linha):
            header = df.row(linha)
            linha_inicial = linha + 1
            break

    df = df[linha_inicial:]
    df.columns = header

    header = [
        'nome', 'codigo_municipio', 'gentilico',
        'prefeito', 'area_km2', 'populacao_residente',
        'densidade_demografica', 'escolarizacao_6_14',
        'idhm', 'mortalidade_infantil',
        'total_receitas_brutas_realizadas', 'total_despesas_brutas_empenhadas',
        'pib_per_capita'
    ]

    df.columns = header

    df = df.drop(['gentilico', 'prefeito'])

    uf = path_csv.split('/')[-1].split('.')[0].upper()

    df = df.with_columns(
        pl.col('area_km2').str.replace('-', '0').cast(pl.Float64),
        pl.col('populacao_residente').str.replace('-', '0').cast(pl.Float64),
        pl.col('densidade_demografica').str.replace('-', '0').cast(pl.Float64),
        pl.col('escolarizacao_6_14').str.replace('-', '0').cast(pl.Float64),
        pl.col('idhm').str.replace('-', '0').cast(pl.Float64),
        pl.col('mortalidade_infantil').str.replace('-', '0').cast(pl.Float64),
        pl.col('total_receitas_brutas_realizadas').str.replace('-', '0').cast(pl.Float64),
        pl.col('total_despesas_brutas_empenhadas').str.replace('-', '0').cast(pl.Float64),
        pl.col('pib_per_capita').str.replace('-', '0').cast(pl.Float64),
        pl.lit(uf).alias('uf')
    )

    # Remove linhas de rodapé (onde coluna "Município" está nula ou onde aparece 'Notas:')
    if 'codigo_municipio' in df.columns:
        df = df.filter(pl.col('codigo_municipio').is_not_null() & ~pl.col('codigo_municipio').str.contains('Notas', literal=True))
    elif 'codigo_municipio' in df.columns:
        df = df.filter(pl.col('codigo_municipio').is_not_null() & ~pl.col('codigo_municipio').str.contains('Notas', literal=True))

    return df

def tratar_codigos_municipais(df, nome_base):
    """
    Atribui a alguns municipios do Distrito Federal que não estão cadastrados no IBGE
    o código 5300108, pois eles pertencem a Brasília.
    """
    codigos = [
        '530170', '530040', '530060', '530180',
        '530050', '530130', '530090', '530070',
        '530080', '530120', '530135', '530100',
        '530020', '530150'
    ]

    print("Corrigindo Codigo dos Municípios do DF na tabela: ", nome_base)
    df = df.with_columns([
        pl.col('CO_ESTADO_GESTOR').cast(pl.Utf8),
        pl.col('CO_MUNICIPIO_GESTOR').cast(pl.Utf8)
    ])

    df = df.with_columns(
        pl.when(pl.col('CO_MUNICIPIO_GESTOR').is_in(codigos))
        .then(pl.lit('530010'))
        .otherwise(pl.col('CO_MUNICIPIO_GESTOR'))
        .alias('CO_MUNICIPIO_GESTOR')
    )

    return df

def adicionar_coluna_data(df, nome_arquivo):
    """
    Adiciona a coluna 'data_competencia' ao DataFrame com base no nome do arquivo.
    """
    ano_mes = nome_arquivo.split('.')[0][-6:]  # Captura os últimos 6 caracteres do nome do arquivo (YYYYMM)
    ano = ano_mes[:4]
    mes = ano_mes[-2:]
    df = df.with_columns(
        pl.concat_str(
            pl.lit(ano),
            pl.lit('-'),
            pl.lit(mes),
            pl.lit('-01')
        ).alias('data_competencia')
    )
    return df

def listar_arquivos(pasta, extensao=''):
    """
    Lista arquivos de uma pasta por extensão.
    """
    return [f for f in os.listdir(pasta) if f.endswith(extensao)]