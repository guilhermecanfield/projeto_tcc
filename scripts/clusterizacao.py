#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline de análise de clusters para o TCC (Disparidades na Oferta de Serviços de Saúde - BR)

O script:
1) Carrega a base consolidada (CSV) e complementos de internações (Parquet);
2) Recria colunas derivadas e indicadores conforme você descreveu;
3) Roda UMAP + HDBSCAN (2 etapas: geral + subclusterização do ruído) com os SEUS parâmetros;
4) Gera embeddings finais de visualização (UMAP 2D) e salva scatter interativo (Plotly);
5) Cria perfis de clusters (média/mediana/desvio);
6) Executa testes não-paramétricos (Kruskal-Wallis) por feature vs. clusters;
7) Roda método alternativo (variância + correlação + KMeans/Hierárquica) e produz matriz de correspondência;
8) Salva todos os artefatos em ./outputs/.

Requisitos: pandas, numpy, polars, scikit-learn, umap-learn, hdbscan, scipy, matplotlib, plotly

Uso:
python analisar_clusters_tcc.py \
  --csv tabela_completa_pivot_202506220918.csv \
  --parquet_internacoes data/internacoes_final/sih_agregado.parquet \
  --n_neighbors 75 --min_cluster_size 75 --min_samples 29 \
  --n_clusters_alt 5 --var_threshold 0.05 --corr_threshold 0.90
"""

import os
import json
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import VarianceThreshold
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics import silhouette_score, confusion_matrix

import umap
import hdbscan

from scipy import stats

import matplotlib.pyplot as plt
import plotly.express as px

# ------------------------
# Utilidades
# ------------------------

def ensure_dir(p: str | Path):
    Path(p).mkdir(parents=True, exist_ok=True)


def kruskal_by_feature(df: pd.DataFrame, features: list[str], cluster_col: str = "cluster") -> pd.DataFrame:
    """Kruskal-Wallis por feature entre grupos de cluster.
    Retorna tabela com H (estatística), p-valor e tamanho de efeito (eta_quadrado_approx).
    """
    rows = []
    groups = [g[1] for g in df.groupby(cluster_col)]
    labels = sorted(df[cluster_col].unique())
    for feat in features:
        samples = [g[feat].dropna().values for g in groups]
        # Garantir que haja variância em todos os grupos
        if any(len(s) < 2 or np.nanvar(s) == 0 for s in samples):
            H, p = np.nan, np.nan
            eta2 = np.nan
        else:
            H, p = stats.kruskal(*samples)
            # Eta^2 de Kruskal aprox = H / (N - 1)
            N = sum(len(s) for s in samples)
            eta2 = H / (N - 1) if N > 1 else np.nan
        rows.append({"feature": feat, "H": H, "p_value": p, "eta2_approx": eta2})
    out = pd.DataFrame(rows).sort_values(["p_value", "H"], na_position="last")
    return out


def correlation_filter(df: pd.DataFrame, threshold: float = 0.90) -> tuple[pd.DataFrame, list[str]]:
    """Remove colunas altamente correlacionadas (pearson) acima do limiar.
    Mantém a primeira ocorrência e remove as subsequentes correlacionadas.
    Retorna o df filtrado e a lista de colunas removidas.
    """
    corr = df.corr(numeric_only=True).abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    to_drop = [column for column in upper.columns if any(upper[column] > threshold)]
    return df.drop(columns=to_drop), to_drop


def hungarian_alignment(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[np.ndarray, dict[int, int]]:
    """Alinha rótulos de y_pred aos de y_true via algoritmo Húngaro com base em confusão.
    Retorna y_pred_alinhado e o mapeamento utilizado.
    """
    try:
        from scipy.optimize import linear_sum_assignment
    except Exception:
        # Fallback simples: retorna os labels originais
        return y_pred, {}

    labels_true = np.unique(y_true)
    labels_pred = np.unique(y_pred)
    C = confusion_matrix(y_true, y_pred, labels=labels_true)
    # Converter para problema de minimização
    cost = C.max() - C
    row_ind, col_ind = linear_sum_assignment(cost)
    mapping = {labels_pred[col_ind[i]]: labels_true[row_ind[i]] for i in range(len(row_ind)) if i < len(col_ind)}
    y_pred_aligned = np.array([mapping.get(lbl, lbl) for lbl in y_pred])
    return y_pred_aligned, mapping


# ------------------------
# Parâmetros de entrada
# ------------------------

parser = argparse.ArgumentParser()
parser.add_argument("--csv", required=True, help="Caminho do CSV principal (tabela_completa_pivot_*.csv)")
parser.add_argument("--parquet_internacoes", required=True, help="Parquet de internações agregado (sih_agregado.parquet)")
parser.add_argument("--outdir", default="outputs", help="Diretório de saída")

# Parâmetros UMAP + HDBSCAN (principal)
parser.add_argument("--n_neighbors", type=int, default=75)
parser.add_argument("--min_cluster_size", type=int, default=75)
parser.add_argument("--min_samples", type=int, default=29)

# Parâmetros método alternativo
parser.add_argument("--n_clusters_alt", type=int, default=5)
parser.add_argument("--var_threshold", type=float, default=0.05)
parser.add_argument("--corr_threshold", type=float, default=0.90)

args = parser.parse_args()
ensure_dir(args.outdir)

# ------------------------
# 1) Carregamento e preparação (seguindo seu passo-a-passo)
# ------------------------

print("[1/9] Carregando CSV principal…")
df = pd.read_csv(args.csv)
df = df.drop(columns=[c for c in ["NO_MUNICIPIO", "CO_MUNICIPIO"] if c in df.columns])
df = df.fillna(0)

if "total_obitos" in df.columns and "obitos_em_estabelecimentos" in df.columns:
    df["obitos_fora_estabelecimento"] = df["total_obitos"] - df["obitos_em_estabelecimentos"]

# normalizar nomes
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(" ", "_", regex=False)

# região e capital
regioes = {
    "Norte":     ["AC", "AM", "AP", "PA", "RO", "RR", "TO"],
    "Nordeste":  ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste":   ["ES", "MG", "RJ", "SP"],
    "Sul":       ["PR", "RS", "SC"]
}

capitais_br = {
    'AC': 'Rio Branco', 'AL': 'Maceió', 'AP': 'Macapá', 'AM': 'Manaus',
    'BA': 'Salvador', 'CE': 'Fortaleza', 'DF': 'Brasília', 'ES': 'Vitória',
    'GO': 'Goiânia', 'MA': 'São Luís', 'MT': 'Cuiabá', 'MS': 'Campo Grande',
    'MG': 'Belo Horizonte', 'PA': 'Belém', 'PB': 'João Pessoa', 'PR': 'Curitiba',
    'PE': 'Recife', 'PI': 'Teresina', 'RJ': 'Rio de Janeiro', 'RN': 'Natal',
    'RS': 'Porto Alegre', 'RO': 'Porto Velho', 'RR': 'Boa Vista', 'SC': 'Florianópolis',
    'SP': 'São Paulo', 'SE': 'Aracaju', 'TO': 'Palmas'
}

uf_sr = set(sum(regioes.values(), []))
if "uf" in df.columns and "nome" in df.columns:
    def mapear_regiao(uf):
        for reg, estados in regioes.items():
            if uf in estados:
                return reg
        return "Região Desconhecida"
    df["regiao"] = df["uf"].apply(mapear_regiao)
    df["capital"] = df["nome"] == df["uf"].map(capitais_br)

print("[2/9] Lendo Parquet de internações e juntando…")
pl.enable_string_cache()
base_pl = pl.from_pandas(df)

internacoes = pl.read_parquet(args.parquet_internacoes).fill_null(0)
if "codigo_municipio" not in base_pl.columns or "codigo_municipio" not in internacoes.columns:
    raise ValueError("A coluna 'codigo_municipio' deve existir em ambas as bases para o join.")

base_pl = base_pl.join(internacoes, on="codigo_municipio", how="left")

print("[3/9] Recriando taxa_ocupacao_anual e preenchendo nulos…")
base_pl = base_pl.with_columns(
    (
        (
            pl.col('total_dias_permanencia').fill_null(0) /
            (pl.col('leitos_existentes').fill_null(0) * 365)
        ) * 100
    ).round(2).alias('taxa_ocupacao_anual')
)

colunas_para_preencher = [
    'total_internacoes','obitos_por_internacao','total_hospitais','total_dias_permanencia',
    'total_diarias','total_valor','valor_medio_diaria','total_obitos_em_internacao',
    'total_municipio_atendidos'
]

fill_exprs = []
for col in colunas_para_preencher:
    if col in base_pl.columns:
        fill_exprs.append(
            pl.when(pl.col(col).is_null()).then(0.0).otherwise(pl.col(col)).alias(col)
        )
if fill_exprs:
    base_pl = base_pl.with_columns(fill_exprs)

# Se não há leitos, taxa de ocupação = 0
if 'leitos_existentes' in base_pl.columns and 'taxa_ocupacao_anual' in base_pl.columns:
    base_pl = base_pl.with_columns(
        pl.when(pl.col('leitos_existentes') == 0)
        .then(pl.lit(0.0))
        .otherwise(pl.col('taxa_ocupacao_anual'))
        .alias('taxa_ocupacao_anual')
    )

# ------------------------
# 2) Definição dos grupos de features
# ------------------------

cols_atencao_basica = [
    "centro_de_saude/unidade_basica",
    "polo_academia_da_saude",
    "posto_de_saude",
    "centro_de_apoio_a_saude_da_familia",
]
cols_complexidade = [
    "unidade_de_apoio_diagnose_e_terapia_(sadt_isolado)",
    "centro_de_atencao_psicossocial",
    "hospital_especializado",
    "policlinica",
    "hospital_geral",
    "pronto_socorro_geral",
]
cols_vigilancia_movel = [
    "unidade_de_vigilancia_em_saude",
    "unidade_movel_terrestre",
    "unidade_movel_fluvial",
    "central_de_regulacao_medica_das_urgencias",
]

base_pl = base_pl.with_columns([
    pl.sum_horizontal(cols_atencao_basica).alias("indice_atencao_basica"),
    pl.sum_horizontal(cols_complexidade).alias("indice_complexidade"),
    pl.sum_horizontal(cols_vigilancia_movel).alias("indice_vigilancia_movel"),
])

features_saude = [
    'unidades_por_k_hab','medicos_por_k_habitante','enfermeiros_por_k_habitante',
    'quantidade_unidades_com_leito_por_k_hab','leitos_sus_por_k_hab','taxa_mortalidade_geral',
    'mortalidade_infantil','pronto_atendimento','telessaude',
    'centro_de_atencao_hemoterapia_e_ou_hematologica','laboratorio_de_saude_publica',
    'centro_de_parto_normal_-_isolado','central_de_regulacao_do_acesso',
    'servico_de_atencao_domiciliar_isolado(home_care)','pronto_socorro_especializado',
    'oficina_ortopedica','farmacia','unidade_de_atencao_em_regime_residencial',
    'central_de_gestao_em_saude','cooperativa_ou_empresa_de_cessao_de_trabalhadores_na_saude',
    'hospital/dia_-_isolado','clinica/centro_de_especialidade','unidade_de_atencao_a_saude_indigena',
    'polo_de_prevencao_de_doencas_e_agravos_e_promocao_da_saude','laboratorio_central_de_saude_publica_lacen',
    'central_de_abastecimento','centro_de_imunizacao','unidade_movel_de_nivel_pre-hospitalar_na_area_de_urgencia',
    'unidade_mista','obitos_por_internacao','total_diarias','valor_medio_diaria',
    'total_municipio_atendidos','taxa_ocupacao_anual',
    # Especialidades / contagens
    "centro_de_saude/unidade_basica","polo_academia_da_saude","posto_de_saude","centro_de_apoio_a_saude_da_familia",
    "unidade_de_apoio_diagnose_e_terapia_(sadt_isolado)","centro_de_atencao_psicossocial","hospital_especializado",
    "policlinica","hospital_geral","pronto_socorro_geral","unidade_de_vigilancia_em_saude","unidade_movel_terrestre",
    "unidade_movel_fluvial","central_de_regulacao_medica_das_urgencias",
]

features_socio_economicas = [
    'populacao','idh','area_territorial','densidade_demografica','matriculas_ensino_medio',
    'total_receitas_brutas','total_despesas_brutas','pib_per_capita','taxa_de_alfabetizados',
    'taxa_de_nao_alfabetizados','pct_coleta_lixo','pct_com_rede_esgoto','pct_sem_rede_esgoto',
    'media_anos_estudo_geral','media_anos_estudo_11_14','media_anos_estudo_15_17',
    'media_anos_estudo_18_24','media_anos_estudo_25_mais','taxa_adultos_sem_instrucao',
    'taxa_adultos_fundamental_completo','taxa_adultos_medio_completo','taxa_adultos_superior_completo',
    'pct_idoso','taxa_pop_residente_favela','taxa_populacao_urbana','taxa_populacao_rural','taxa_freq_escolar'
]

label_cols = [c for c in ["codigo_municipio","nome","uf","regiao"] if c in base_pl.columns]

# Converte para pandas para modelagem
base = base_pl.to_pandas()

# Subset das features realmente presentes
features_saude = [c for c in features_saude if c in base.columns]

print(f"[4/9] Padronizando {len(features_saude)} features de saúde…")
X = base[features_saude].copy()
scaler = StandardScaler()
X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=features_saude, index=base.index)

# ------------------------
# 3) UMAP + HDBSCAN (principal) com dois estágios
# ------------------------

print("[5/9] Rodando UMAP + HDBSCAN (etapa 1)…")
reducer_main = umap.UMAP(n_components=2, random_state=42, n_neighbors=args.n_neighbors)
Z_main = reducer_main.fit_transform(X_scaled)

clusterer_main = hdbscan.HDBSCAN(min_cluster_size=args.min_cluster_size, min_samples=args.min_samples)
labels_main = clusterer_main.fit_predict(Z_main)

base["cluster"] = labels_main
print("Contagem de clusters (etapa 1):\n", base["cluster"].value_counts().sort_index())

print("[5b/9] Subclusterizando ruído (cluster = -1)…")
mask_noise = base["cluster"] == -1
if mask_noise.any():
    Z_noise = umap.UMAP(n_components=2, random_state=42, n_neighbors=args.n_neighbors).fit_transform(X_scaled.loc[mask_noise])
    subclusterer = hdbscan.HDBSCAN(min_cluster_size=21, min_samples=20)
    labels_sub = subclusterer.fit_predict(Z_noise)

    # Remap para IDs novos e unir
    max_lab = base["cluster"].max()
    offset = (max_lab if pd.notna(max_lab) else -1) + 1
    labels_sub_remap = np.array([lbl + offset if lbl != -1 else -1 for lbl in labels_sub])
    base.loc[mask_noise, "cluster"] = labels_sub_remap

print("Contagem de clusters (final):\n", base["cluster"].value_counts().sort_index())

# Embedding final para visualização
print("[6/9] Gerando UMAP (2D) para visualização…")
Z_vis = umap.UMAP(n_components=2, random_state=42, n_neighbors=args.n_neighbors).fit_transform(X_scaled)
base["umap_x"], base["umap_y"] = Z_vis[:,0], Z_vis[:,1]

# Scatter plotly
ensure_dir(args.outdir)
fig = px.scatter(
    base,
    x="umap_x", y="umap_y",
    color=base["cluster"].astype(str),
    hover_name=base.get("nome", None),
    title="Clusterização Final dos Municípios (UMAP + HDBSCAN)",
)
fig.update_layout(legend_title_text="Cluster", height=760)
fig.write_html(Path(args.outdir)/"scatter_umap_hdbscan.html", include_plotlyjs="cdn")

# ------------------------
# 4) Perfis dos clusters
# ------------------------

print("[7/9] Criando perfis descritivos dos clusters…")
perfil_mean = base.groupby("cluster")[features_saude].mean().round(3)
perfil_median = base.groupby("cluster")[features_saude].median().round(3)
perfil_std = base.groupby("cluster")[features_saude].std(ddof=0).round(3)

perfil_mean.to_csv(Path(args.outdir)/"perfil_clusters_media.csv", encoding="utf-8")
perfil_median.to_csv(Path(args.outdir)/"perfil_clusters_mediana.csv", encoding="utf-8")
perfil_std.to_csv(Path(args.outdir)/"perfil_clusters_desvio.csv", encoding="utf-8")

# Boxplots simples em Matplotlib (um arquivo por feature)
box_dir = Path(args.outdir)/"boxplots"
ensure_dir(box_dir)
for feat in features_saude:
    plt.figure()
    base.boxplot(column=feat, by="cluster", grid=False)
    plt.suptitle("")
    plt.title(f"{feat} por cluster")
    plt.xlabel("cluster")
    plt.ylabel(feat)
    plt.tight_layout()
    plt.savefig(box_dir/f"box_{feat}.png", dpi=120)
    plt.close()

# ------------------------
# 5) Testes estatísticos (Kruskal)
# ------------------------

print("[8/9] Rodando Kruskal-Wallis por feature…")
kr = kruskal_by_feature(base, features_saude, cluster_col="cluster")
kr.to_csv(Path(args.outdir)/"kruskal_clusters.csv", index=False, encoding="utf-8")

# ------------------------
# 6) Método alternativo (variância + correlação + KMeans/Hierárquica)
# ------------------------

print("[9/9] Método alternativo para robustez…")

# Seleção por variância
vt = VarianceThreshold(threshold=args.var_threshold)
X_vt = vt.fit_transform(X_scaled)
cols_vt = [col for col, keep in zip(features_saude, vt.get_support().tolist()) if keep]

# Filtro por correlação (sobre df reduzido)
X_vt_df = pd.DataFrame(X_vt, columns=cols_vt, index=X_scaled.index)
X_corr_df, dropped_corr = correlation_filter(X_vt_df, threshold=args.corr_threshold)

# KMeans
km = KMeans(n_clusters=args.n_clusters_alt, n_init=20, random_state=42)
labels_km = km.fit_predict(X_corr_df)

# Hierárquica Aglomerativa
agg = AgglomerativeClustering(n_clusters=args.n_clusters_alt, linkage="ward")
labels_agg = agg.fit_predict(X_corr_df)

# Silhouette (só para referência)
try:
    sil_km = silhouette_score(X_corr_df, labels_km)
    sil_agg = silhouette_score(X_corr_df, labels_agg)
except Exception:
    sil_km = np.nan
    sil_agg = np.nan

# Matriz de correspondência com rótulos do método principal
cross_km = pd.crosstab(base["cluster"], labels_km)
cross_agg = pd.crosstab(base["cluster"], labels_agg)

# Tentar alinhar (Hungarian) para facilitar leitura
labels_km_aligned, mapping_km = hungarian_alignment(base["cluster"].values, labels_km)
labels_agg_aligned, mapping_agg = hungarian_alignment(base["cluster"].values, labels_agg)

cross_km_aligned = pd.crosstab(base["cluster"], labels_km_aligned)
cross_agg_aligned = pd.crosstab(base["cluster"], labels_agg_aligned)

# Salvar saídas
meta = {
    "umap": {"n_neighbors": args.n_neighbors},
    "hdbscan": {"min_cluster_size": args.min_cluster_size, "min_samples": args.min_samples},
    "alt": {
        "n_clusters": args.n_clusters_alt,
        "var_threshold": args.var_threshold,
        "corr_threshold": args.corr_threshold,
        "features_pos_var": cols_vt,
        "features_pos_corr": X_corr_df.columns.tolist(),
        "features_removidas_por_corr": dropped_corr,
        "silhouette_kmeans": float(sil_km) if pd.notna(sil_km) else None,
        "silhouette_agglomerative": float(sil_agg) if pd.notna(sil_agg) else None,
        "mapping_kmeans": mapping_km,
        "mapping_agglomerative": mapping_agg,
    },
}

base_out = base[label_cols + features_saude].copy() if label_cols else base.copy()
base_out["cluster"] = base["cluster"]
base_out["umap_x"] = base["umap_x"]
base_out["umap_y"] = base["umap_y"]

base_out.to_csv(Path(args.outdir)/"municipios_clusterizados.csv", index=False, encoding="utf-8")

cross_km.to_csv(Path(args.outdir)/"matriz_correspondencia_kmeans.csv", encoding="utf-8")
cross_agg.to_csv(Path(args.outdir)/"matriz_correspondencia_agglomerative.csv", encoding="utf-8")
cross_km_aligned.to_csv(Path(args.outdir)/"matriz_correspondencia_kmeans_alinhada.csv", encoding="utf-8")
cross_agg_aligned.to_csv(Path(args.outdir)/"matriz_correspondencia_agglomerative_alinhada.csv", encoding="utf-8")

with open(Path(args.outdir)/"metadados.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)

print("\n✅ Concluído! Arquivos salvos em:", Path(args.outdir).resolve())
