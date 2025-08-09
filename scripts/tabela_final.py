"""
Constrói a mesma `tabela_final` da Query Principal usando **Polars (Lazy)**,
sem depender do DuckDB para a etapa de agregação final.

Pré‑requisitos (diretórios conforme `scripts/configs.py`):
- DIRS['CONCAT_CNES']  → parquets CNES deduplicados (ex.: tbestabelecimento_2022.parquet ...)
- DIRS['FINAL_CIDADES']→ tb_cidades_ibge_2022.parquet
- DIRS['FINAL_MORTALIDADE'] → mortalidade_2022.parquet
- DIRS['FINAL_IBGE']   → parquets adicionais do IBGE (taxas etc.)

Saída:
- `data/tabela_final/tabela_final.parquet` (mesmo path usado no seu pipeline)
- opcional: CSV em `data/tabela_final/tabela_final.csv`
"""
from __future__ import annotations
import os
import polars as pl

# -----------------------------------------------------------------------------
# Config: tenta importar seus caminhos; caso não encontre, usa defaults locais
# -----------------------------------------------------------------------------
try:
    from scripts.configs import DIRS, ANO
except Exception:
    DIRS = {
        'CONCAT_CNES': 'data/cnes_concatenados',
        'FINAL_CIDADES': 'data/cidades_final',
        'FINAL_MORTALIDADE': 'data/mortalidade_final',
        'FINAL_IBGE': 'data/ibge_final',
        'TABELA_FINAL': 'data/tabela_final',
    }
    ANO = '2022'

os.makedirs(DIRS['TABELA_FINAL'], exist_ok=True)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def readp(*parts: str) -> pl.LazyFrame:
    """Lê parquet em LazyFrame."""
    path = os.path.join(*parts)
    return pl.scan_parquet(path)


def safe_read_ibge(name_no_ext: str) -> pl.LazyFrame:
    """Tenta ler um parquet do diretório FINAL_IBGE com diferentes variações de nome."""
    candidates = [
        f"{name_no_ext}.parquet",
        f"{name_no_ext.lower()}.parquet",
        f"{name_no_ext.upper()}.parquet",
    ]
    for c in candidates:
        full = os.path.join(DIRS['FINAL_IBGE'], c)
        if os.path.exists(full):
            return pl.scan_parquet(full)
    raise FileNotFoundError(f"Parquet IBGE não encontrado para base: {name_no_ext}")


# -----------------------------------------------------------------------------
# Carregamentos principais (Lazy)
# -----------------------------------------------------------------------------

estab = readp(DIRS['CONCAT_CNES'], f"tbestabelecimento_{ANO}.parquet")
mun   = readp(DIRS['CONCAT_CNES'], f"tbmunicipio_{ANO}.parquet")
mun = mun.with_columns(
    pl.col("CO_MUNICIPIO").cast(pl.Utf8)
)
atu   = readp(DIRS['CONCAT_CNES'], f"tbatividade_{ANO}.parquet")
atu = atu.with_columns(
    pl.col('CO_ATIVIDADE').cast(pl.Utf8)
)
uni   = readp(DIRS['CONCAT_CNES'], f"tbtipounidade_{ANO}.parquet")
test  = readp(DIRS['CONCAT_CNES'], f"tbtipoestabelecimento_{ANO}.parquet")
test = test.with_columns(
    pl.col("CO_TIPO_ESTABELECIMENTO").cast(pl.Utf8)
)
rl    = readp(DIRS['CONCAT_CNES'], f"rlestabcomplementar_{ANO}.parquet")
prof  = readp(DIRS['CONCAT_CNES'], f"tbatividadeprofissional_{ANO}.parquet")
chs   = readp(DIRS['CONCAT_CNES'], f"tbcargahorariasus_{ANO}.parquet")

mortal = readp(DIRS['FINAL_MORTALIDADE'], "mortalidade_2022.parquet")
cidades = readp(DIRS['FINAL_CIDADES'], "cidades_ibge_2022.parquet")

# Tabelas IBGE adicionais (nomes conforme usados na SQL)
ibge_taxa_alf = safe_read_ibge("taxa_de_alfabetizacao")
ibge_coleta   = safe_read_ibge("taxa_coleta_lixo")
ibge_esgoto   = safe_read_ibge("taxa_rede_esgoto")
ibge_estudo   = safe_read_ibge("anos_de_estudo")
ibge_instr    = safe_read_ibge("taxa_populacao_nivel_instrucao")
ibge_favela   = safe_read_ibge("pop_res_favela")
ibge_dom      = safe_read_ibge("taxa_situacao_domicilio")
ibge_freq     = safe_read_ibge("taxa_frequencia_escolar")
ibge_etaria   = safe_read_ibge("taxa_distribuicao_etaria")

# -----------------------------------------------------------------------------
# CTE: leitos (apenas estabelecimentos ativos, competência 2022-12-01)
# -----------------------------------------------------------------------------

estab_ativos = estab.filter(pl.col("CO_MOTIVO_DESAB") == "")

leitos = (
    rl.filter(pl.col("data_competencia") == "2022-12-01")
      .join(estab_ativos.select("CO_UNIDADE").unique(), on="CO_UNIDADE", how="inner")
      .group_by("CO_UNIDADE")
      .agg([
          pl.col("QT_EXIST").sum().alias("leitos_existentes"),
          pl.col("QT_SUS").sum().alias("leitos_sus"),
      ])
)

# -----------------------------------------------------------------------------
# CTE: tabela_completa
# -----------------------------------------------------------------------------

# joins de dicionários
estab_base = (
    estab_ativos
    .join(mun.select(["CO_MUNICIPIO", "NO_MUNICIPIO", "CO_SIGLA_ESTADO"]), left_on="CO_MUNICIPIO_GESTOR", right_on="CO_MUNICIPIO", how="left")
    .join(uni.select(["CO_TIPO_UNIDADE", "DS_TIPO_UNIDADE"]), left_on="TP_UNIDADE", right_on="CO_TIPO_UNIDADE", how="left")
    .join(test.select(["CO_TIPO_ESTABELECIMENTO", "DS_TIPO_ESTABELECIMENTO"]), on="CO_TIPO_ESTABELECIMENTO", how="left")
    .join(atu.select(["CO_ATIVIDADE", "DS_ATIVIDADE", "DS_CONCEITO_ATIVIDADE"]), left_on="CO_ATIVIDADE_PRINCIPAL", right_on="CO_ATIVIDADE", how="left")
    .join(leitos, on="CO_UNIDADE", how="left")
)

# -----------------------------------------------------------------------------
# CTE: óbitos por estabelecimento e totais por município
# -----------------------------------------------------------------------------

obitos_por_estab = (
    mortal
    .group_by("CODESTAB")
    .agg(pl.len().alias("qtd_obitos"))
)

# cuidado de tipos para join CO_CNES (int) vs CODESTAB (string)
estab_com_obitos = (
    estab_base
    .with_columns(
        pl.col("CO_CNES").cast(pl.Int64)
    )
    .join(
        obitos_por_estab.with_columns(pl.col("CODESTAB").cast(pl.Int64)),
        left_on="CO_CNES", right_on="CODESTAB", how="left"
    )
)

obitos_total = (
    mortal
    .join(mun.select(["CO_MUNICIPIO"]).unique(), left_on="CODMUNOCOR", right_on="CO_MUNICIPIO", how="inner")
    .group_by("CO_MUNICIPIO")
    .agg(pl.len().alias("total_obitos"))
    # .rename({"CO_MUNICIPIO": "codigo"})
)

# -----------------------------------------------------------------------------
# CTE: estabelecimentos com leito & por município
# -----------------------------------------------------------------------------

estab_com_leito = (
    estab_base.filter(pl.col("leitos_existentes") > 0)
    .group_by("CO_MUNICIPIO")
    .agg(pl.len().alias("quantidade_unidades_com_leito"))
)

estab_por_mun = (
    estab.group_by("CO_MUNICIPIO_GESTOR").agg(pl.len().alias("quantidade_unidades"))
)

# -----------------------------------------------------------------------------
# CTE: socio_economicos (join IBGE cidades + contagem de estabelecimentos)
# -----------------------------------------------------------------------------

socio = (
    cidades
    .join(estab_por_mun, left_on="codigo_municipio", right_on="CO_MUNICIPIO_GESTOR", how="inner")
    .with_columns([
        (pl.col("populacao_residente") / pl.col("quantidade_unidades")).alias("hab_por_unidade"),
        (pl.col("quantidade_unidades") / pl.col("populacao_residente")).alias("unidades_por_hab"),
        ((pl.col("quantidade_unidades") / pl.col("populacao_residente")) * 1000).alias("unidades_por_k_hab"),
    ])
    .select([
        pl.col("uf"),
        pl.col("codigo_municipio"),
        pl.col("nome"),
        pl.col("hab_por_unidade"),
        pl.col("unidades_por_hab"),
        pl.col("unidades_por_k_hab"),
        pl.col("quantidade_unidades"),
        pl.col("area_km2").alias("area_territorial"),
        pl.col("populacao_residente").alias("populacao"),
        pl.col("densidade_demografica"),
        pl.col("escolarizacao_6_14").alias("matriculas_ensino_medio"),
        pl.col("idhm").alias("idh"),
        pl.col("total_receitas_brutas_realizadas").alias("total_receitas_brutas"),
        pl.col("total_despesas_brutas_empenhadas").alias("total_despesas_brutas"),
        pl.col("pib_per_capita"),
        pl.col("mortalidade_infantil"),
    ])
    .rename({'codigo_municipio': 'CO_MUNICIPIO'})
)

# -----------------------------------------------------------------------------
# CTE: profissionais, médicos, enfermeiros
# -----------------------------------------------------------------------------

profissionais = (
    chs
    .join(estab.select(["CO_UNIDADE", "NO_FANTASIA", "CO_MUNICIPIO_GESTOR"]).rename({"NO_FANTASIA": "nome_fantasia"}), on="CO_UNIDADE", how="inner")
    .join(prof.select(["CO_CBO", "DS_ATIVIDADE_PROFISSIONAL", "TP_CLASSIFICACAO_PROFISSIONAL", "TP_CBO_SAUDE"]).rename({
        "DS_ATIVIDADE_PROFISSIONAL": "atividade_profissional",
        "TP_CLASSIFICACAO_PROFISSIONAL": "classificacao_profissional",
        "TP_CBO_SAUDE": "cbo_saude",
    }), on="CO_CBO", how="inner")
    .join(mun.select(["CO_MUNICIPIO", "NO_MUNICIPIO", "CO_SIGLA_ESTADO"]).rename({
        "CO_MUNICIPIO": "codigo_municipio",
        "NO_MUNICIPIO": "municipio",
        "CO_SIGLA_ESTADO": "uf",
    }), left_on="CO_MUNICIPIO_GESTOR", right_on="codigo_municipio", how="inner")
    .select([
        pl.col("CO_UNIDADE").alias("codigo_unidade"),
        pl.col("nome_fantasia"),
        pl.col("codigo_municipio"),
        pl.col("municipio"),
        pl.col("uf"),
        pl.col("CO_PROFISSIONAL_SUS").alias("codigo_profissional"),
        pl.col("atividade_profissional"),
        pl.col("classificacao_profissional"),
        pl.col("cbo_saude"),
        pl.col("TP_SUS_NAO_SUS").alias("sus"),
    ])
)

medicos = (
    profissionais
    .filter(pl.col("atividade_profissional").str.contains("(?i)medic", literal=False))
    .group_by("codigo_municipio")
    .agg(pl.col("codigo_profissional").n_unique().alias("qtd_medicos"))
    .rename({'codigo_municipio': 'CO_MUNICIPIO'})
)

enfermeiros = (
    profissionais
    .filter(pl.col("atividade_profissional").str.contains("(?i)enfermei", literal=False))
    .group_by("codigo_municipio")
    .agg(pl.col("codigo_profissional").n_unique().alias("qtd_enfermeiros"))
    .rename({'codigo_municipio': 'CO_MUNICIPIO'})
)

# -----------------------------------------------------------------------------
# Montagem da tabela_final (joins + agregações)
# -----------------------------------------------------------------------------

# PIVOT de tipos de unidade (contagem por município)
pivot_unidades = (
    estab_base
    .select(["CO_MUNICIPIO", "DS_TIPO_UNIDADE"])
    .with_columns(pl.col("DS_TIPO_UNIDADE").fill_null("DESCONHECIDO"))
    .group_by(["CO_MUNICIPIO", "DS_TIPO_UNIDADE"])
    .agg(pl.len().alias("qtd_unidades"))
    .collect()  # vira DataFrame
    .pivot(
        index="CO_MUNICIPIO",
        columns="DS_TIPO_UNIDADE",
        values="qtd_unidades",
        aggregate_function="sum",
    )
    .lazy()     # volta a ser LazyFrame pra poder fazer joins depois
)

# Bloco principal
base_join = (
    estab_com_obitos
    .join(socio,         left_on="CO_MUNICIPIO", right_on="codigo_municipio", how="left")
    .join(obitos_total,  left_on="CO_MUNICIPIO", right_on="codigo", how="left")
    .join(ibge_taxa_alf, left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_coleta,   left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_esgoto,   left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_estudo,   left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_instr,    left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_favela,   left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_dom,      left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_freq,     left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(ibge_etaria,   left_on=pl.col("CO_MUNICIPIO").cast(pl.Utf8), right_on=pl.col("codigo").str.slice(0, 6), how="left")
    .join(medicos,       left_on="CO_MUNICIPIO", right_on="codigo_municipio", how="left")
    .join(enfermeiros,   left_on="CO_MUNICIPIO", right_on="codigo_municipio", how="left")
    .join(estab_com_leito, on="CO_MUNICIPIO", how="left")
)

# Agregações finais por município
agg = (
    base_join
    .group_by([
        "uf", "codigo_municipio", "nome", "populacao", "idh",
        "mortalidade_infantil", "area_territorial", "densidade_demografica",
        "matriculas_ensino_medio", "total_receitas_brutas", "total_despesas_brutas",
        "pib_per_capita"
    ])
    .agg([
        pl.col("CO_UNIDADE").n_unique().alias("qtd_unidades"),
        pl.col("hab_por_unidade").first().round(2),
        (pl.col("CO_UNIDADE").n_unique() / pl.col("populacao").max() * 1000).alias("unidades_por_k_hab"),
        ((pl.col("qtd_medicos") / pl.col("populacao")) * 1000).alias("medicos_por_k_habitante"),
        (pl.col("populacao") / pl.col("qtd_medicos")).alias("habitantes_por_medico"),
        ((pl.col("qtd_enfermeiros") / pl.col("populacao")) * 1000).alias("enfermeiros_por_k_habitante"),
        (pl.col("populacao") / pl.col("qtd_enfermeiros")).alias("habitantes_por_enfermeiros"),
        pl.col("leitos_existentes").sum().alias("leitos_existentes"),
        pl.col("quantidade_unidades_com_leito").sum().alias("quantidade_unidades_com_leito"),
        (pl.col("quantidade_unidades_com_leito").sum() / pl.col("populacao").sum() * 1000).alias("quantidade_unidades_com_leito_por_k_hab"),
        (pl.col("leitos_existentes").sum() / pl.col("CO_UNIDADE").n_unique()).round(2).alias("total_leitos_unidade"),
        (pl.col("leitos_existentes").sum() / pl.col("populacao").max() * 1000).alias("leitos_por_k_hab"),
        pl.col("leitos_sus").sum().alias("leitos_sus"),
        (pl.col("leitos_sus").sum() / pl.col("CO_UNIDADE").n_unique()).round(2).alias("total_leitos_sus_unidade"),
        (pl.col("leitos_sus").sum() / pl.col("populacao").max() * 1000).alias("leitos_sus_por_k_hab"),
        (pl.col("qtd_obitos").sum() / pl.col("CO_UNIDADE").n_unique()).round(2).alias("obitos_por_unidade"),
        pl.col("qtd_obitos").sum().alias("obitos_em_estabelecimentos"),
        pl.col("total_obitos").first().alias("total_obitos"),
        ((pl.col("total_obitos") / pl.col("populacao")).first() * 1000).round(2).alias("taxa_mortalidade_geral"),
        # Colunas IBGE adicionais (mantidas como first, pois 1:1 por município)
        pl.col("total").first().alias("taxa_de_alfabetizados"),
        (100 - pl.col("total").first()).alias("taxa_de_nao_alfabetizados"),
        pl.col("pct_coletado").first().alias("pct_coleta_lixo"),
        pl.col("sim").first().alias("pct_com_rede_esgoto"),
        pl.col("nao").first().alias("pct_sem_rede_esgoto"),
        pl.col("11_14").first().alias("media_anos_estudo_11_14"),
        pl.col("15_17").first().alias("media_anos_estudo_15_17"),
        pl.col("18_24").first().alias("media_anos_estudo_18_24"),
        pl.col("25_mais").first().alias("media_anos_estudo_25_mais"),
        pl.col("sem_instrucao_fundamental_incompleto").first().alias("taxa_adultos_sem_instrucao"),
        pl.col("fundamental_completo_medio_incompleto").first().alias("taxa_adultos_fundamental_completo"),
        pl.col("medio_completo_superior_incompleto").first().alias("taxa_adultos_medio_completo"),
        pl.col("superior_completo").first().alias("taxa_adultos_superior_completo"),
        pl.col("pct_60_mais").first().alias("pct_idoso"),
        (pl.col("total")/pl.col("populacao").first()).alias("taxa_pop_residente_favela"),
        pl.col("urbana").first().alias("taxa_populacao_urbana"),
        pl.col("rural").first().alias("taxa_populacao_rural"),
        pl.col("total_right").first().alias("taxa_freq_escolar"),
        pl.col("total_left").first().alias("media_anos_estudo_geral"),
    ])
)

# Observação: as colunas "total_left"/"total_right" acima podem variar conforme o schema
# dos parquets IBGE. Se os nomes divergirem, ajuste os aliases conforme suas colunas reais.

# Join com PIVOT dos tipos de unidade
result = (
    agg.join(
        pivot_unidades,
        left_on="codigo_municipio",
        right_on="CO_MUNICIPIO",
        how="left",
    )
)

# -----------------------------------------------------------------------------
# Materializa e salva
# -----------------------------------------------------------------------------

df_final = result.collect()

out_parquet = os.path.join(DIRS['TABELA_FINAL'], "tabela_final.parquet")
df_final.write_parquet(out_parquet)

# opcional: CSV
try:
    out_csv = os.path.join(DIRS['TABELA_FINAL'], "tabela_final.csv")
    df_final.write_csv(out_csv)
except Exception:
    pass

print(f"OK! Salvo em: {out_parquet}")
