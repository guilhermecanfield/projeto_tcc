# scripts/download_data.py

from ftplib import FTP
import zipfile
import os
from scripts.utils import criar_pastas, configurar_logger

logger = configurar_logger()

# Diretórios
RAW_DIR = 'data/raw'
criar_pastas([RAW_DIR])

TABELAS_INTERESSE = [
    'tbEstabelecimento', 'rlEstabComplementar', 'tbAtividade', 
    'tbAtividadeProfissional', 'tbMunicipio', 'tbEstado', 
    'tbTipoUnidade', 'tbTipoEstabelecimento', 'tbAtributo',
    'tbCargaHorariaSus'
]

def baixar_e_extrair_cnes(ano='2022'):
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('cnes')

    for mes in range(1, 13):
        mes_str = f'{mes:02d}'
        arquivo = f'BASE_DE_DADOS_CNES_{ano}{mes_str}.zip'
        caminho_local = os.path.join(RAW_DIR, arquivo)
        logger.info(f"Baixando {arquivo}...")

        with open(caminho_local, 'wb') as f:
            ftp.retrbinary(f'RETR {arquivo}', f.write)

        with zipfile.ZipFile(caminho_local, 'r') as zip_ref:
            for arquivo_zip in zip_ref.namelist():
                if any(tab.lower() in arquivo_zip.lower() for tab in TABELAS_INTERESSE):
                    zip_ref.extract(arquivo_zip, RAW_DIR)
                    logger.info(f"Extraído: {arquivo_zip}")

    # Remove o .zip após a extração
        os.remove(caminho_local)
        logger.info(f"Arquivo {arquivo} removido após extração.")

    ftp.quit()
    logger.info("Download e extração finalizados.")

if __name__ == "__main__":
    baixar_e_extrair_cnes()