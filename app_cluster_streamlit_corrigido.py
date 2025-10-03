
import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import geobr

st.set_page_config(
    page_title="Mapa da Saúde no Brasil",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="auto"
)

@st.cache_data
def carregar_dados():
    df = pd.read_parquet('dados/municipios_clusterizados.parquet')
    df['codigo_municipio'] = df['codigo_municipio'].astype(str)
    df['cluster'] = df['cluster'].astype(str)  # <- conversão para string
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].round(2)
    return df

@st.cache_data
def carregar_geodados():
    municipios = geobr.read_municipality()
    municipios['code_muni_abrev'] = municipios['code_muni'].astype(str).str.slice(0, 6)
    municipios['geometry'] = municipios['geometry'].simplify(tolerance=0.01)
    return municipios

def merge_dados_geograficos(df, geodf):
    gdf = geodf.merge(df, left_on='code_muni_abrev', right_on='codigo_municipio', how='left')
    gdf['cluster'] = gdf['cluster'].astype(str)  # <- conversão para string
    return gdf

def obter_cores_clusters(num_clusters):
    cores_vibrantes = [
        '#e41a1c', '#377eb8', '#4daf4a', '#ff7f00', '#984ea3',
        '#f781bf', '#a65628', '#ffff33', '#00CED1', '#FF1493',
    ]
    if num_clusters <= len(cores_vibrantes):
        return cores_vibrantes[:num_clusters]
    return px.colors.sample_colorscale("rainbow", [n/(num_clusters-1) for n in range(num_clusters)])

df = carregar_dados()
geodf = carregar_geodados()
gdf = merge_dados_geograficos(df, geodf)

st.title("🗺️ Mapa de Clusters - Acesso à Saúde nos Municípios Brasileiros")
st.markdown("Este mapa exibe os municípios brasileiros agrupados por similaridade de indicadores de saúde e socioeconômicos.")

clusters_disponiveis = sorted(df['cluster'].dropna().unique().tolist())
clusters_selecionados = st.multiselect(
    'Selecione os clusters:',
    options=clusters_disponiveis,
    default=clusters_disponiveis
)

col1, col2 = st.columns(2)
with col1:
    ufs = sorted(df['uf'].unique().tolist())
    uf_selecionada = st.selectbox('UF:', ['Todas as UFs'] + ufs)

with col2:
    if uf_selecionada != 'Todas as UFs':
        municipios = sorted(df[df['uf'] == uf_selecionada]['nome'].unique().tolist())
    else:
        municipios = sorted(df['nome'].unique().tolist())
    municipio_selecionado = st.selectbox('Município:', ['Todos os municípios'] + municipios)

if clusters_selecionados:
    gdf_filtrado = gdf[gdf['cluster'].isin(clusters_selecionados)]
    if uf_selecionada != 'Todas as UFs':
        gdf_filtrado = gdf_filtrado[gdf_filtrado['uf'] == uf_selecionada]
    if municipio_selecionado != 'Todos os municípios':
        gdf_filtrado = gdf_filtrado[gdf_filtrado['nome'] == municipio_selecionado]

    if len(gdf_filtrado) > 0:
        cores = obter_cores_clusters(len(clusters_disponiveis))
        mapa_cores = {c: cores[i] for i, c in enumerate(clusters_disponiveis)}

        fig = px.choropleth_map(
            gdf_filtrado,
            geojson=gdf_filtrado.geometry,
            locations=gdf_filtrado.index,
            color='cluster',
            color_discrete_map=mapa_cores,
            hover_name='nome',
            hover_data=['uf', 'populacao', 'idh', 'cluster'],
            zoom=3.5,
            center={"lat": -15.77972, "lon": -52.92972},
            opacity=0.7,
            title='🗺️ Mapa de Clusters por Município',
            category_orders={"cluster": clusters_disponiveis}
        )
        fig.update_layout(mapbox_style="carto-positron", height=800)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nenhum município encontrado para os filtros.")
else:
    st.warning("Selecione ao menos um cluster para visualizar.")
