import os
import polars as pl
from scripts.utils import criar_pastas, listar_arquivos

# Pastas
RAW_DIR = 'data/raw'
PARQUET_DIR = 'data/parquet'

def extrair_transformar_salvar_parquets():
    """
    Lê arquivos CSV extraídos do CNES, trata e salva em Parquet.
    """
    criar_pastas([PARQUET_DIR])

    arquivos_csv = listar_arquivos(RAW_DIR, extensao='.csv')

    for arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(RAW_DIR, arquivo)
        
        print(f'📄 Lendo {arquivo}...')
        try:
            # Polars lê CSV muito rápido
            df = pl.read_csv(
                caminho_arquivo,
                separator=';', 
                encoding='latin1',  # padrão dos dados do Datasus
                infer_schema_length=10000  # aumenta inferência para tabelas grandes
            )
        except Exception as e:
            print(f'❌ Erro ao ler {arquivo}: {e}')
            continue

        # Padronizar colunas
        df = df.rename({col: col.strip().lower().replace(' ', '_') for col in df.columns})

        # Nome base para salvar
        nome_base = arquivo.replace('.csv', '').lower()

        caminho_parquet = os.path.join(PARQUET_DIR, f'{nome_base}.parquet')

        print(f'💾 Salvando {nome_base}.parquet...')
        df.write_parquet(caminho_parquet)

    print('✅ Extração e transformação finalizadas.')

if __name__ == "__main__":
    extrair_transformar_salvar_parquets()