import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px

# Configurar estilo personalizado
st.set_page_config(
    page_title="Mapa da Saúde no Brasil",
    page_icon="🗺️",
    layout="wide", #centered
    initial_sidebar_state="auto"
)

# --- Funções auxiliares ---
@st.cache_data
def carregar_dados():
    df = pd.read_parquet('dados/municipios_clusterizados.parquet')
    df['uf_nome'] = df['uf'] + ' - ' + df['nome']
    df['codigo_municipio'] = df['codigo_municipio'].astype(str)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].round(2)
    return df

@st.cache_data
def carregar_geodados():
    return gpd.read_parquet("dados/municipios_geobr.parquet")

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
    'cluster': 'Cluster',
    'populacao': '👥 População',
    'idh': '📊 IDH',
    'pib_per_capita': '💰 PIB per capita',
    'taxa_de_alfabetizados': '📚 Taxa de alfabetização',
    'pct_idoso': '👴 % de idosos',
    'taxa_freq_escolar': '📖 Taxa de frequência escolar',
    'taxa_populacao_urbana': '🏙️ Taxa de população urbana',
    'unidades_por_k_hab': '🏥 Unidades de Saúde por mil hab.',
    'medicos_por_k_habitante': '🩺 Médicos por mil hab.',
    'enfermeiros_por_k_habitante': '🧑‍⚕️ Enfermeiros por mil hab.',
    'leitos_por_k_hab': '🛏️ Leitos por mil hab.',
    'leitos_sus_por_k_hab': '🛏️ Leitos SUS por mil hab.',
    'taxa_mortalidade_geral': '💀 Mortalidade geral',
    'mortalidade_infantil': '👶 Mortalidade infantil',
}

# --- Interface ---

st.write("<h1 style='text-align: center;'>🗺️ Mapa da Saúde no Brasil</h1>", unsafe_allow_html=True)

st.write("""
    Este mapa interativo exibe os clusters de municípios brasileiros baseados em indicadores de saúde e socioeconômicos.
    
    Os clusters agrupam municípios com características similares em relação a:
    - População e estrutura demográfica
    - Infraestrutura de saúde (unidades, médicos, enfermeiros, leitos)
    - Indicadores de saúde (mortalidade geral e infantil)
    - Indicadores socioeconômicos (IDH, PIB per capita, educação)

    Você pode filtrar por estado, município e selecionar quais clusters deseja visualizar no mapa.
    
    💡 **Dica:** passe o mouse sobre um município para ver os **KPIs no hover** (Cluster, 👥 População, 📊 IDH, 💰 PIB per capita, 🏥 Unidades/1k hab., 🩺 Médicos/1k hab., 🧑‍⚕️ Enfermeiros/1k hab., 🛏️ Leitos gerais/SUS, 💀 Mortalidade geral, 👶 Mortalidade infantil).

    **Obs.: O mapa pode demorar até 1 minuto para ser exibido!**
""")

col1, col2, col3 = st.columns(3)

with col1:
    opcoes_select = ['Todos os Clusters'] + df['cluster'].sort_values().unique().tolist()

    cluster = st.selectbox('Selecione o Cluster:', options=opcoes_select)

    df_selecionado = df[df['cluster'] == cluster] if cluster != 'Todos os Clusters' else df.copy()

    gdf_selecionado = gdf[gdf['cluster'] == cluster] if cluster != 'Todos os Clusters' else gdf.copy()

with col2:
    ufs = sorted(df['uf'].unique().tolist())
    uf_selecionada = st.selectbox('Selecione a UF:', options=ufs + ['Todas as UFs'], index=len(ufs))

    if uf_selecionada != 'Todas as UFs':
        municipios_disponiveis = sorted(df_selecionado[df_selecionado['uf'] == uf_selecionada]['nome'].unique().tolist())
    else:
        municipios_disponiveis = sorted(df_selecionado['nome'].unique().tolist())

with col3:
    municipio_selecionado = st.selectbox('Selecione o município:', options=municipios_disponiveis + ['Todos os municípios'], index=len(municipios_disponiveis))

    if uf_selecionada != 'Todas as UFs':
        gdf_selecionado = gdf_selecionado[gdf_selecionado['uf'] == uf_selecionada]
    if municipio_selecionado != 'Todos os municípios':
        gdf_selecionado = gdf_selecionado[gdf_selecionado['nome'] == municipio_selecionado]

fig = px.choropleth_map(
    gdf_selecionado,
    geojson=gdf_selecionado.geometry,
    locations=gdf_selecionado.index,
    color='cluster',
    color_continuous_scale=obter_cores(inverter=True),
    range_color=[0, 8],
    zoom=4,
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
    legend=dict(
        title="Clusters",
        orientation="v",        # legenda vertical
        yanchor="top",
        y=1,
        xanchor="left",
        x=0.01,
        bgcolor="rgba(255,255,255,0.7)",  # fundo semi-transparente
        bordercolor="black",
        borderwidth=1
    )
)

st.plotly_chart(fig, use_container_width=True)

# --- Seção de Contato ---
st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento adicional
st.markdown("---")  # Linha divisória

st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento adicional
st.subheader(["📊 Estatísticas Gerais do Brasil" if cluster == 'Todos os Clusters' else f"📊 Estatísticas do Cluster {cluster}"][0])
st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento adicional

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total de Municípios", f"{len(df_selecionado):,}")
with col2:
    st.metric("Total de Estados", f"{df_selecionado['uf'].nunique():,}")
with col3:
    st.metric("População Total", f"{df_selecionado['populacao'].sum():,.0f}")
with col4:
    st.metric("IDH Médio", f"{df_selecionado['idh'].mean():.3f}")
with col5:
    st.markdown("Top 5 Estados do Cluster")
    st.table(df_selecionado.groupby('uf').agg({'codigo_municipio': 'nunique', 'populacao': 'sum'}).head(5).rename(columns={'uf': 'UF', 'codigo_municipio': 'Nº Mun.', 'populacao': 'Pop.'}).sort_values(by='Nº Mun.', ascending=False))

st.markdown("<h3 style='text-align: center; color: #1f77b4;'>📬 Entre em Contato</h3>", unsafe_allow_html=True)

# Criar colunas para organizar os links
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div style='text-align: center; padding: 10px;'>
        <a href='https://www.linkedin.com/in/guilhermecanfield/' target='_blank' style='text-decoration: none;'>
            <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border: 2px solid #e0e0e0;'>
                <div style='font-size: 30px; margin-bottom: 5px;'>💼</div>
                <div style='color: #333; font-weight: bold;'>LinkedIn</div>
            </div>
        </a>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style='text-align: center; padding: 10px;'>
        <a href='https://sites.google.com/view/guilhermecanfield/' target='_blank' style='text-decoration: none;'>
            <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border: 2px solid #e0e0e0;'>
                <div style='font-size: 30px; margin-bottom: 5px;'>🌐</div>
                <div style='color: #333; font-weight: bold;'>Portfólio</div>
            </div>
        </a>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style='text-align: center; padding: 10px;'>
        <a href='https://github.com/guilhermecanfield' target='_blank' style='text-decoration: none;'>
            <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border: 2px solid #e0e0e0;'>
                <div style='font-size: 30px; margin-bottom: 5px;'>💻</div>
                <div style='color: #333; font-weight: bold;'>GitHub</div>
            </div>
        </a>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div style='text-align: center; padding: 10px;'>
        <a href='mailto:guilherme.canfield87@gmail.com' style='text-decoration: none;'>
            <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border: 2px solid #e0e0e0;'>
                <div style='font-size: 30px; margin-bottom: 5px;'>📧</div>
                <div style='color: #333; font-weight: bold;'>Email</div>
            </div>
        </a>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<p style='text-align: center; color: #666; font-style: italic; margin-top: 20px;'>Desenvolvido por Guilherme Canfield de Almeida | Dados de Saúde Pública do Brasil</p>", unsafe_allow_html=True)