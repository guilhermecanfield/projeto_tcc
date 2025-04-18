# 🏥 Projeto de Análise das Disparidades na Saúde no Brasil

Este repositório é parte de um projeto acadêmico que visa **analisar as disparidades regionais na oferta de serviços de saúde** no Brasil, utilizando dados do **CNES**, **IBGE**, **SUS** e outras bases oficiais.

Atualmente, estamos na **Fase 1** do projeto:  
📦 **Extração e Transformação dos Dados**

O projeto completo contempla três fases:

---

# 📌 Fase 1 - Engenharia de Dados

Objetivo: Preparar todos os dados necessários para a análise, de forma estruturada, eficiente e pronta para ciência de dados.

- Conexão com o FTP do DataSUS.
- Download dos dados do CNES (Cadastro Nacional de Estabelecimentos de Saúde) para todo o ano de 2022.
- Extração apenas das tabelas relevantes.
- Padronização e transformação dos dados para o formato **Parquet**.
- Organização dos dados em um banco de dados **DuckDB** (`database.duckdb`).

> 📁 Estrutura de dados organizada em:
> - `/data/raw/` → arquivos CSV extraídos
> - `/data/parquet/` → arquivos limpos e otimizados
> - `/data/database.duckdb` → banco relacional para análises

---

# 📌 Fase 2 - Análise Exploratória de Dados (EDA)

Objetivo: Explorar e entender o comportamento dos dados.

- Estatísticas descritivas.
- Visualizações geográficas e demográficas.
- Análise de variáveis socioeconômicas versus infraestrutura de saúde.
- Identificação de padrões e anomalias regionais.

---

# 📌 Fase 3 - Ciência de Dados (PCA e Clusterização)

Objetivo: Identificar grupos de municípios com características semelhantes.

- Aplicação de **PCA** (Análise de Componentes Principais) para redução de dimensionalidade.
- Realização de **Clusterização** (ex.: K-Means, HDBSCAN).
- Interpretação dos grupos formados.
- Geração de **insights acadêmicos** sobre as disparidades regionais.

---

# 🚀 Tecnologias Utilizadas

- Python 3.11+
- Polars
- DuckDB
- pyarrow
- ftplib (biblioteca padrão do Python)
- zipfile (biblioteca padrão do Python)

---

# 🛠️ Como Executar

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu_usuario/projeto_tcc.git
   cd projeto_tcc

2. Ative seu ambiente virtual:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows

3. Instale as dependências:
   ```bash
   poetry install

4. Execute o pipeline completo:
   ```bash
   python scripts/main.py

# 📋 Status Atual do Projeto

| Fase | Status |
|:---|:---|
| 📦 Engenharia de Dados (Extração e Transformação) | ✅ Finalizada |
| 🔍 Análise Exploratória de Dados (EDA) | 🔜 Em breve |
| 🧠 Ciência de Dados (PCA e Clusterização) | 🔜 Em breve |

# 🎯 Objetivo Final

Fornecer subsídios acadêmicos robustos sobre as **disparidades regionais na oferta de serviços de saúde no Brasil**, apoiados em técnicas modernas de análise de dados.
