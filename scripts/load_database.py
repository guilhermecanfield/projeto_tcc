import os
import duckdb
import polars as pl
from scripts.utils import listar_arquivos

# Pastas
PARQUET_DIR = 'data/parquet'
DATABASE_PATH = 'data/database.duckdb'

def carregar_parquets_para_duckdb():
    """
    Lê Parquets transformados e carrega no banco DuckDB.
    """
    print(f'🐥 Abrindo conexão no banco {DATABASE_PATH}...')
    con = duckdb.connect(database=DATABASE_PATH, read_only=False)

    arquivos_parquet = listar_arquivos(PARQUET_DIR, extensao='.parquet')

    for arquivo in arquivos_parquet:
        caminho_arquivo = os.path.join(PARQUET_DIR, arquivo)
        nome_tabela = arquivo.replace('.parquet', '').lower()

        print(f'📄 Carregando {nome_tabela} para o banco...')

        # Lê o Parquet
        df = pl.read_parquet(caminho_arquivo)

        # Salva no DuckDB (substitui se já existir)
        con.execute(f"CREATE OR REPLACE TABLE {nome_tabela} AS SELECT * FROM df")

    con.close()
    print('✅ Todos os dados foram carregados para o banco!')

if __name__ == "__main__":
    carregar_parquets_para_duckdb()