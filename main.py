from scripts.download_data import baixar_e_extrair_cnes
from scripts.extract_transform import extrair_transformar_salvar_parquets
from scripts.load_database import carregar_parquets_para_duckdb

if __name__ == "__main__":
    print("🚀 Iniciando pipeline completo...")

    baixar_e_extrair_cnes()
    extrair_transformar_salvar_parquets()
    carregar_parquets_para_duckdb()

    print("✅ Pipeline finalizado com sucesso!")