import os

def criar_pastas(pastas):
    """
    Cria as pastas necessárias caso não existam.
    """
    for pasta in pastas:
        os.makedirs(pasta, exist_ok=True)

def listar_arquivos(pasta, extensao=''):
    """
    Lista arquivos de uma pasta por extensão.
    """
    return [f for f in os.listdir(pasta) if f.endswith(extensao)]