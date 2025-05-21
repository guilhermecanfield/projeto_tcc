import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import geobr

# Configurar estilo personalizado
st.set_page_config(
    page_title="Mapa da Saúde no Brasil",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="auto"
)

# --- Funções auxiliares ---
@st.cache_data
def carregar_dados():
    df = pd.read_parquet('dados/tabela_final.parquet').fillna(0)
    df['uf_nome'] = df['uf'] + ' - ' + df['nome']
    df['obitos_fora_estabelecimento'] = df['total_obitos'] - df['obitos_em_estabelecimentos']
    df['taxa_obitos_fora_estabelecimento'] = (df['obitos_fora_estabelecimento'] / df['total_obitos']) * 100
    df['codigo_municipio'] = df['codigo_municipio'].astype(str)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].round(2)
    return df

@st.cache_data
def carregar_geodados():
    municipios = geobr.read_municipality()
    municipios['code_muni_abrev'] = municipios['code_muni'].astype(str).str.slice(0, 6)
    return municipios

def merge_dados_geograficos(df, geodf):
    return geodf.merge(df, left_on='code_muni_abrev', right_on='codigo_municipio', how='left')

def obter_cores(inverter=False):
    escala = [
        [0, 'rgb(165,0,38)'],
        [0.25, 'rgb(215,48,39)'],
        [0.5, 'rgb(254,224,144)'],
        [0.75, 'rgb(116,196,118)'],
        [1, 'rgb(35,139,69)']
    ]
    return escala[::-1] if inverter else escala

# --- Dados e labels ---
df = carregar_dados()
geodf = carregar_geodados()
gdf = merge_dados_geograficos(df, geodf)

labels = {
    'populacao': '👥 População',
    'idh': '📊 IDH',
    'unidades_por_k_hab': '🏥 Unidades de Saúde por mil hab.',
    'medicos_por_k_habitante': '🩺 Médicos por mil hab.',
    'enfermeiros_por_k_habitante': '🧑‍⚕️ Enfermeiros por mil hab.',
    'leitos_por_k_hab': '🛏️ Leitos por mil hab.',
    'leitos_sus_por_k_hab': '🛏️ Leitos SUS por mil hab.',
    'taxa_mortalidade_geral': '💀 Mortalidade geral',
    'mortalidade_infantil': '👶 Mortalidade infantil',
    'pib_per_capita': '💰 PIB per capita',
    'taxa_de_alfabetizados': '📚 Taxa de alfabetização',
    'pct_idoso': '👴 % de idosos',
    'taxa_freq_escolar': '📖 Taxa de frequência escolar',
    'taxa_populacao_urbana': '🏙️ Taxa de população urbana'
}

colunas_selecionaveis = [
    'unidades_por_k_hab', 'idh', 'medicos_por_k_habitante',
    'enfermeiros_por_k_habitante', 'leitos_por_k_hab',
    'taxa_mortalidade_geral', 'mortalidade_infantil',
    'pib_per_capita', 'taxa_de_alfabetizados', 'pct_idoso',
    'taxa_freq_escolar', 'taxa_populacao_urbana'
]
labels_filtrados = {col: labels[col] for col in colunas_selecionaveis}

# --- Interface ---

st.write("<h1 style='text-align: center;'>🗺️ Mapa de Acesso à Saúde nos Municípios Brasileiros</h1>", unsafe_allow_html=True)
st.write("""
    Este mapa apresenta dados sobre o acesso à saúde nos municípios brasileiros.
    Os dados incluem população, unidades de saúde, médicos, enfermeiros, leitos, óbitos e mortalidade infantil.
    
    **Obs.: O mapa pode demorar até 1 minuto para ser exibido!**
""")

opcoes_kpi = ['Selecione o critério'] + list(labels_filtrados.values())
kpi_label = st.selectbox('Selecione o indicador que irá determinar a coloração do mapa:', options=opcoes_kpi)

if kpi_label != 'Selecione o critério':
    coluna_kpi = {v: k for k, v in labels.items()}[kpi_label]
    inverter_cores = coluna_kpi in ['obitos_por_unidade', 'taxa_mortalidade_geral', 'mortalidade_infantil']
    cores = obter_cores(inverter=inverter_cores)

    ufs = sorted(df['uf'].unique().tolist())
    uf_selecionada = st.selectbox('Selecione a UF:', options=ufs + ['Todas as UFs'], index=len(ufs))

    if uf_selecionada != 'Todas as UFs':
        municipios_disponiveis = sorted(df[df['uf'] == uf_selecionada]['nome'].unique().tolist())
    else:
        municipios_disponiveis = sorted(df['nome'].unique().tolist())
    municipio_selecionado = st.selectbox('Selecione o município:', options=municipios_disponiveis + ['Todos os municípios'], index=len(municipios_disponiveis))

    gdf_filtrado = gdf.copy()
    if uf_selecionada != 'Todas as UFs':
        gdf_filtrado = gdf_filtrado[gdf_filtrado['uf'] == uf_selecionada]
    if municipio_selecionado != 'Todos os municípios':
        gdf_filtrado = gdf_filtrado[gdf_filtrado['nome'] == municipio_selecionado]

    # Título da legenda
    partes_legenda = kpi_label.split()
    titulo_legenda = '<br>'.join([' '.join(partes_legenda[:2]), ' '.join(partes_legenda[2:])])

    fig = px.choropleth_map(
        gdf_filtrado,
        geojson=gdf_filtrado.geometry,
        locations=gdf_filtrado.index,
        color=coluna_kpi,
        color_continuous_scale=cores,
        range_color=[0, gdf_filtrado[coluna_kpi].quantile(0.95)],
        zoom=3.5,
        center={"lat": -15.77972, "lon": -52.92972},
        opacity=0.7,
        hover_name='uf_nome',
        hover_data=list(labels.keys()),
        labels=labels,
        title='🗺️ Mapa de Acesso à Saúde nos Municípios Brasileiros'
    )

    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        height=800,
        coloraxis_colorbar=dict(
            title=titulo_legenda,
            thicknessmode="pixels", thickness=20,
            lenmode="pixels", len=300,
            yanchor="top", y=1,
            ticks="outside"
        )
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Por favor, selecione um critério para visualizar o mapa.")