# config.py

# Diretórios
DIRS = {
    'RAW_CNES': 'data/raw',
    'PARQUET_CNES': 'data/parquet',
    'CONCAT_CNES': 'data/cnes_concatenados',
    'FINAL_CNES': 'data/cnes_final',
    'FINAL_CIDADES': 'data/cidades_final',
    'FINAL_MORTALIDADE': 'data/mortalidade_final',
    'FINAL_IBGE': 'data/ibge_final',
    'BASE_CIDADES': 'data/cidades/',
    'BASE_MORTALIDADE': 'data/sim',
    'BASE_IBGE': 'data/ibge',
    'TABELA_FINAL': 'data/tabela_final'
}

# Anos
ANO = '2022'

# Caminho do banco de dados
DB_PATH = 'data/database.duckdb'

# Caminho dos arquivos de log
LOG_FILE = 'logs/application.log'
