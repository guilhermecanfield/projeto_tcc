# 🏥 Mapeamento das Desigualdades na Oferta de Serviços de Saúde entre Municípios Brasileiros

📌 [LinkedIn](https://www.linkedin.com/in/guilhermecanfield) • 🌐 [Portfólio Profissional](https://sites.google.com/view/guilhermecanfield/)

---


Este repositório reúne o projeto de conclusão do MBA em **Data Science e Analytics pela USP/Esalq**, cujo objetivo foi **identificar e mapear as disparidades regionais na oferta de serviços de saúde no Brasil**, com base em dados públicos do **CNES**, **IBGE**, **SIM/SUS** e **IPEA**.

---

## 🗺️ Destaque: Mapa Interativo com Clusterização

🔍 Acesse agora o **mapa interativo** com todos os municípios brasileiros agrupados por similaridade, segundo análise de componentes principais e clusterização não supervisionada:

👉 **[mapasaudebrasil.streamlit.app](https://mapadasaudenobrasil.streamlit.app/)**

> Nele, é possível visualizar **a que cluster cada município pertence**, navegar pelos perfis regionais, e explorar desigualdades na infraestrutura de saúde com clareza.

---

## 📊 Visão Geral do Projeto

O projeto foi desenvolvido em três grandes fases:

### 📌 Fase 1 – Engenharia de Dados

🔧 Preparação de uma base robusta e estruturada para análise:

- Conexão automática com o FTP do DataSUS;
- Download e transformação de dados do **CNES (2022)**;
- Integração com dados socioeconômicos do **IBGE**, indicadores de mortalidade (**SIM/SUS**) e dados do **IPEA**;
- Padronização e conversão para o formato **Parquet**;
- Criação de banco analítico **DuckDB** local com todas as tabelas integradas;
- Geração da tabela final consolidada com +90 variáveis por município.

📁 Estrutura organizada em:

```
/data/raw/                  # Dados originais (CSV, XLSX, ZIP)
│
├── /cnes/                  # Dados CNES tratados
├── /ibge_final/            # Dados IBGE e IPEA integrados
├── /mortalidade_final/     # Óbitos SIM/SUS por estabelecimento
├── /parquet/               # Versões intermediárias otimizadas
├── /tabela_final/          # Tabela principal para análise
└── database.duckdb         # Banco relacional para consultas
```

---

### 📌 Fase 2 – Análise Exploratória de Dados (EDA)

🔍 Exploração detalhada dos dados integrados:

- Estatísticas descritivas e visualizações geográficas;
- Correlações entre indicadores demográficos, estruturais e socioeconômicos;
- Construção de rankings municipais por capacidade de atendimento;
- Heatmaps temáticos por cluster e comparação com mediana nacional.

---

### 📌 Fase 3 – Ciência de Dados (UMAP + HDBSCAN)

🧠 Aplicação de técnicas avançadas para segmentar os municípios brasileiros com base em múltiplos indicadores:

- Redução de dimensionalidade com **UMAP (Uniform Manifold Approximation and Projection)**, técnica não linear que preserva a estrutura local dos dados e facilita a visualização em 2D;
- Clusterização com **HDBSCAN (Hierarchical Density-Based Spatial Clustering of Applications with Noise)**, que permite identificar grupos com formatos arbitrários e lida bem com ruídos;
- Reclassificação de pontos rotulados inicialmente como ruído (cluster -1) em novos clusters válidos com base em características semelhantes;
- Interpretação dos agrupamentos com base nos indicadores originais, incluindo perfis socioeconômicos, demográficos e de infraestrutura de saúde.

> A abordagem UMAP + HDBSCAN foi escolhida por sua capacidade de **detectar padrões complexos e regiões heterogêneas** de forma não supervisionada e robusta.

---

## 🚀 Tecnologias Utilizadas

- Python 3.11+
- [Polars](https://www.pola.rs/)
- [DuckDB](https://duckdb.org/)
- [HDBSCAN](https://hdbscan.readthedocs.io/en/latest/)
- [UMAP-learn](https://umap-learn.readthedocs.io/en/latest/)
- [Streamlit](https://streamlit.io/)
- Plotly, Seaborn, PyArrow, ftplib, zipfile, logging
- Poetry (gerenciador de ambiente e dependências)

---

## 🛠️ Como Executar Localmente

1. Clone o repositório:
   ```bash
   git clone https://github.com/guilhermecanfield/projeto_tcc.git
   cd projeto_tcc
   ```

2. Crie e ative seu ambiente virtual:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate   # Windows
   ```

3. Instale as dependências via Poetry:
   ```bash
   poetry install
   ```

4. Execute o pipeline completo:
   ```bash
   python main.py
   ```

---

## 📋 Status Final do Projeto

| Etapa | Status |
|:--|:--|
| 📦 Engenharia de Dados | ✅ Concluída |
| 🔍 Análise Exploratória de Dados (EDA) | ✅ Concluída |
| 🧠 Modelagem (UMAP + HDBSCAN) | ✅ Concluída |
| 🗺️ Mapa Interativo no Streamlit | ✅ Publicado |
| 📝 TCC Entregue | ✅ Finalizado |

---

## 🎯 Objetivo Final

Fornecer subsídios acadêmicos e analíticos sobre as **desigualdades na infraestrutura de saúde pública no Brasil**, por meio de:

- Integração de bases públicas complexas e dispersas;
- Aplicação de métodos modernos de ciência de dados;
- Entrega de insights práticos e visualmente acessíveis para pesquisadores, gestores e cidadãos.

---

📫 Dúvidas ou sugestões? Entre em contato via [LinkedIn](https://www.linkedin.com/in/guilhermecanfield) ou abra uma issue aqui no repositório.