# main.py

from scripts.download_data import baixar_e_extrair_cnes
from scripts.extract_transform import (
    transformar_dados,
    concatenar_parquets_por_tabela,
    tratar_e_deduplicar_tabelas,
    tratar_estabelecimentos,
    trata_dados_cidades,
    tratar_dados_mortalidade
)
from scripts.load_database import (
    carregar_parquets_para_duckdb,
    carregar_cidades_ibge_no_duckdb,
    carregar_mortalidade_no_duckdb
)

def main():
    print("\nIniciando pipeline completo de Engenharia de Dados...\n")

    # 1. Download dos dados brutos
    # baixar_e_extrair_cnes()

    # 2. Transformação CSV -> Parquet
    transformar_dados()

    # 3. Concatenar todos os Parquets de 2022
    concatenar_parquets_por_tabela()

    # 4. Tratar e deduplicar tabelas
    tratar_e_deduplicar_tabelas()

    # 5. Tratar estabelecimentos ativos
    tratar_estabelecimentos()

    # 6. Tratar dados de cidades do IBGE
    trata_dados_cidades()

    # 7. Tratar dados de mortalidade
    tratar_dados_mortalidade()

    # 8. Carregar Parquets no banco de dados DuckDB
    carregar_parquets_para_duckdb()

    # 9. Carregar cidades do IBGE no banco de dados DuckDB
    carregar_cidades_ibge_no_duckdb()

    # 10. Carregar dados de mortalidade no banco de dados DuckDB
    carregar_mortalidade_no_duckdb()

    print("\nPipeline completo finalizado com sucesso!\n")

if __name__ == "__main__":
    main()