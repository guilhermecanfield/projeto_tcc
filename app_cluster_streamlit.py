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
    df = pd.read_parquet('dados/municipios_clusterizados.parquet')
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

def obter_cores_clusters(num_clusters):
    """Retorna uma paleta de cores distinta e vibrante para os clusters"""
    # Paleta de cores vibrantes e bem contrastantes
    cores_vibrantes = [
        '#e41a1c',  # Vermelho vibrante
        '#377eb8',  # Azul forte
        '#4daf4a',  # Verde vibrante
        '#ff7f00',  # Laranja forte
        '#984ea3',  # Roxo
        '#f781bf',  # Rosa
        '#a65628',  # Marrom
        '#ffff33',  # Amarelo
        '#00CED1',  # Turquesa
        '#FF1493',  # Pink forte
    ]
    
    if num_clusters <= len(cores_vibrantes):
        return cores_vibrantes[:num_clusters]
    
    # Se precisar de mais cores, gera uma paleta expandida
    return px.colors.sample_colorscale("rainbow", [n/(num_clusters-1) for n in range(num_clusters)])

# --- Dados e labels ---
df = carregar_dados()
geodf = carregar_geodados()
gdf = merge_dados_geograficos(df, geodf)

labels = {
    'cluster': '🎯 Cluster',
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
st.write("<h1 style='text-align: center;'>🗺️ Mapa de Acesso à Saúde nos Municípios Brasileiros</h1>", unsafe_allow_html=True)

st.write("""
    Este mapa interativo exibe os clusters de municípios brasileiros baseados em indicadores de saúde e socioeconômicos.
    
    Os clusters agrupam municípios com características similares em relação a:
    - População e estrutura demográfica
    - Infraestrutura de saúde (unidades, médicos, enfermeiros, leitos)
    - Indicadores de saúde (mortalidade geral e infantil)
    - Indicadores socioeconômicos (IDH, PIB per capita, educação)

    Você pode filtrar por estado, município e selecionar quais clusters deseja visualizar no mapa.
    
    **Obs.: O mapa pode demorar até 1 minuto para ser exibido!**
""")

# Obter clusters únicos e ordenar
clusters_disponiveis = sorted(df['cluster'].dropna().unique().tolist())

# Seleção de clusters
st.subheader("🎯 Filtrar por Clusters")
clusters_selecionados = st.multiselect(
    'Selecione os clusters que deseja visualizar:',
    options=clusters_disponiveis,
    default=clusters_disponiveis,
    help="Deixe todos selecionados para visualizar o mapa completo"
)

# Filtros geográficos
col_filtro1, col_filtro2 = st.columns(2)

with col_filtro1:
    ufs = sorted(df['uf'].unique().tolist())
    uf_selecionada = st.selectbox(
        'Selecione a UF:',
        options=['Todas as UFs'] + ufs,
        index=0
    )

with col_filtro2:
    if uf_selecionada != 'Todas as UFs':
        municipios_disponiveis = sorted(df[df['uf'] == uf_selecionada]['nome'].unique().tolist())
    else:
        municipios_disponiveis = sorted(df['nome'].unique().tolist())
    
    municipio_selecionado = st.selectbox(
        'Selecione o município:',
        options=['Todos os municípios'] + municipios_disponiveis,
        index=0
    )

# Filtrar dados
if clusters_selecionados:
    gdf_filtrado = gdf[gdf['cluster'].isin(clusters_selecionados)].copy()
    
    if uf_selecionada != 'Todas as UFs':
        gdf_filtrado = gdf_filtrado[gdf_filtrado['uf'] == uf_selecionada]
    
    if municipio_selecionado != 'Todos os municípios':
        gdf_filtrado = gdf_filtrado[gdf_filtrado['nome'] == municipio_selecionado]

    # Criar mapa
    if len(gdf_filtrado) > 0:
        # Obter cores para os clusters
        cores_clusters = obter_cores_clusters(len(clusters_disponiveis))
        mapa_cores = {cluster: cores_clusters[i] for i, cluster in enumerate(clusters_disponiveis)}
        
        fig = px.choropleth_map(
            gdf_filtrado,
            geojson=gdf_filtrado.geometry,
            locations=gdf_filtrado.index,
            color='cluster',
            color_discrete_map=mapa_cores,
            zoom=3.5,
            center={"lat": -15.77972, "lon": -52.92972},
            opacity=0.7,
            hover_name='nome',
            hover_data=list(labels.keys()),
            labels=labels,
            title='🗺️ Mapa de Clusters - Acesso à Saúde nos Municípios Brasileiros',
            category_orders={"cluster": clusters_disponiveis}
        )

        fig.update_layout(
            mapbox_style="carto-positron",
            margin={"r": 0, "t": 50, "l": 0, "b": 0},
            height=800,
            legend=dict(
                title=dict(text="Cluster", font=dict(size=14, color="black")),
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255, 255, 255, 0.95)",
                bordercolor="rgba(0, 0, 0, 0.2)",
                borderwidth=1,
                font=dict(size=12)
            )
        )

        st.plotly_chart(fig, use_container_width=True)
        
        # Estatísticas dos clusters selecionados
        st.markdown("---")  # Linha divisória antes das estatísticas
        st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento adicional
        st.subheader("📊 Estatísticas dos Clusters Selecionados")
        st.markdown("<br>", unsafe_allow_html=True)  # Espaçamento adicional
        
        df_filtrado = df[df['cluster'].isin(clusters_selecionados)]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Municípios", f"{len(df_filtrado):,}")
        with col2:
            st.metric("População Total", f"{df_filtrado['populacao'].sum():,.0f}")
        with col3:
            st.metric("IDH Médio", f"{df_filtrado['idh'].mean():.3f}")
        with col4:
            st.metric("Clusters Ativos", len(clusters_selecionados))
    else:
        st.warning("Nenhum município encontrado com os filtros selecionados.")
else:
    st.warning("Por favor, selecione pelo menos um cluster para visualizar o mapa.")

# --- Seção de Contato ---
st.markdown("---")

st.markdown("<h3 style='text-align: center; color: #1f77b4;'>📬 Entre em Contato</h3>", unsafe_allow_html=True)

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