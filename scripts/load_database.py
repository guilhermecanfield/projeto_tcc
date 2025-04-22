# scripts/load_database.py

import duckdb
import os
from scripts.utils import criar_pastas, configurar_logger

logger = configurar_logger()

FINAL_DIR = 'data/cnes'
DB_PATH = 'data/database.duckdb'

def carregar_parquets_para_duckdb():
    logger.info("Carregando Parquets no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    for arquivo in os.listdir(FINAL_DIR):
        if arquivo.endswith('.parquet'):
            nome_tabela = arquivo.replace('_2022.parquet', '')
            caminho = os.path.join(FINAL_DIR, arquivo)

            logger.info(f"Inserindo tabela {nome_tabela}...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE {nome_tabela} AS 
                SELECT * FROM read_parquet('{caminho}')
            """)

    conn.close()
    logger.info("Carga no banco finalizada.")

def carregar_cidades_ibge_no_duckdb():
    conn = duckdb.connect(DB_PATH)

    # Cria a tabela, caso não exista
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tb_cidades_ibge_2022 (
            nome TEXT,
            codigo_municipio TEXT,
            area_km2 DOUBLE,
            populacao_residente DOUBLE,
            densidade_demografica DOUBLE,
            escolarizacao_6_14 DOUBLE,
            idhm DOUBLE,
            mortalidade_infantil DOUBLE,
            total_receitas_brutas_realizadas DOUBLE,
            total_despesas_brutas_empenhadas DOUBLE,
            pib_per_capita DOUBLE,
            uf TEXT
        );
    """)

    # Faz o INSERT dos dados especificando as colunas
    conn.execute("""
        INSERT INTO tb_cidades_ibge_2022 (
            nome,
            codigo_municipio,
            area_km2,
            populacao_residente,
            densidade_demografica,
            escolarizacao_6_14,
            idhm,
            mortalidade_infantil,
            total_receitas_brutas_realizadas,
            total_despesas_brutas_empenhadas,
            pib_per_capita,
            uf
        )
        SELECT
            nome,
            codigo_municipio,
            area_km2,
            populacao_residente,
            densidade_demografica,
            escolarizacao_6_14,
            idhm,
            mortalidade_infantil,
            total_receitas_brutas_realizadas,
            total_despesas_brutas_empenhadas,
            pib_per_capita,
            uf
        FROM read_parquet('data/cidades_final/cidades_ibge_2022.parquet');
    """)

    conn.close()
    print("Tabela tb_cidades_ibge_2022 criada e populada com sucesso no DuckDB!")