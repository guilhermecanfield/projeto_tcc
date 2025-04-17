import os
from ftplib import FTP
import zipfile
from scripts.utils import criar_pastas

# Pastas
RAW_DIR = 'data/raw'
TABELAS_INTERESSE = [
    'rlEstabComplementar',
    'tbAtributo',
    'tbEstabelecimento',
    'tbMunicipio',
    'tbEstado',
    'tbTipoUnidade',
    'tbTipoEstabelecimento',
    'tbAtividade',
    'tbAtividadeProfissional',
]

def baixar_e_extrair_cnes(ano='2022'):
    """
    Faz o download e extração das tabelas do CNES de todas as competências de um ano.
    """
    criar_pastas([RAW_DIR])
    
    # Conecta ao FTP
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('cnes')  # Diretório correto

    # Meses (01 a 12)
    meses = [f'{mes:02d}' for mes in range(1, 13)]

    for mes in meses:
        competencia = f'{ano}{mes}'
        arquivo_zip = f'BASE_DE_DADOS_CNES_{competencia}.zip'
        caminho_local = os.path.join(RAW_DIR, arquivo_zip)

        print(f'🔽 Baixando {arquivo_zip}...')
        with open(caminho_local, 'wb') as f:
            ftp.retrbinary(f'RETR {arquivo_zip}', f.write)

        print(f'📦 Extraindo tabelas de interesse de {arquivo_zip}...')
        with zipfile.ZipFile(caminho_local, 'r') as zip_ref:
            for nome_arquivo in zip_ref.namelist():
                for base in TABELAS_INTERESSE:
                    if nome_arquivo.lower().startswith(base.lower()) and nome_arquivo.lower().endswith('.csv'):
                        zip_ref.extract(nome_arquivo, RAW_DIR)

    ftp.quit()
    print('✅ Download e extração finalizados.')

if __name__ == "__main__":
    baixar_e_extrair_cnes()