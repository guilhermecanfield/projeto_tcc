# scripts/load_database.py

import duckdb
import os
from scripts.utils import configurar_logger, criar_pastas
from scripts.configs import DIRS, DB_PATH

logger = configurar_logger()

def carregar_parquets_para_duckdb():
    logger.info("Carregando tabelas do CNES no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    for arquivo in os.listdir(DIRS['CONCAT_CNES']):
        if arquivo.endswith('.parquet'):
            nome_tabela = arquivo.replace('_2022.parquet', '')
            caminho = os.path.join(DIRS['CONCAT_CNES'], arquivo)

            logger.info(f"Inserindo tabela {nome_tabela}...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE {nome_tabela} AS 
                SELECT * FROM read_parquet('{caminho}')
            """)
    conn.close()
    logger.info("Tabelas do CNES criadas e populadas com sucesso no DuckDB!")

def carregar_cidades_ibge_no_duckdb():
    logger.info("Carregando dados de tb_cidades_ibge_2022 no DuckDB...")
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
    conn.execute(f"""
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
            SUBSTRING(codigo_municipio, 1, 6) AS codigo_municipio,
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
        FROM read_parquet('{DIRS["FINAL_CIDADES"]}/cidades_ibge_2022.parquet');
    """)

    conn.close()
    logger.info("Tabela tb_cidades_ibge_2022 criada e populada com sucesso no DuckDB!")

def carregar_mortalidade_no_duckdb():
    logger.info("Carregando dados de mortalidade no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    caminho = f'{DIRS["FINAL_MORTALIDADE"]}/mortalidade_2022.parquet'

    conn.execute("""
        CREATE OR REPLACE TABLE tb_mortalidade_2022 AS
        SELECT * FROM read_parquet(?)
    """, (caminho,))

    conn.close()
    logger.info("Tabela tb_mortalidade_2022 criada e populada com sucesso no DuckDB!")

def carregar_ibge_adicionais_no_duckdb():
    logger.info("Carregando tabelas adicionais do IBGE no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    for arquivo in os.listdir(DIRS['FINAL_IBGE']):
        if arquivo.endswith('.parquet'):
            nome_tabela = arquivo.replace('.parquet', '')
            caminho = f"{DIRS['FINAL_IBGE']}/{arquivo}"

            print(nome_tabela)

            logger.info(f"Inserindo tabela {nome_tabela}...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE {nome_tabela} AS 
                SELECT * FROM read_parquet('{caminho}')
            """)

    conn.close()
    logger.info("Tabelas adicionais do IBGE carregadas com sucesso no DuckDB!")
