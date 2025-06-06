# 🏥 Projeto de Análise das Disparidades na Saúde no Brasil

Este repositório faz parte do meu projeto de conclusão do MBA em **Data Science e Analytics pela USP/Esalq**. O objetivo é **analisar as disparidades regionais na oferta de serviços de saúde no Brasil**, utilizando dados públicos do **CNES**, **IBGE**, **SUS (SIM)** e **IPEA**.

Atualmente, estamos na **Fase 1** do projeto:  
📦 **Extração e Transformação dos Dados**

O projeto completo contempla três grandes etapas:

---

## 📌 Fase 1 - Engenharia de Dados

🔧 Objetivo: preparar todos os dados necessários para as análises, de forma estruturada, padronizada e otimizada para uso em ciência de dados.

- Conexão com o FTP do DataSUS.
- Download dos dados do **CNES** (Cadastro Nacional de Estabelecimentos de Saúde) para todo o ano de 2022.
- Extração seletiva das tabelas relevantes.
- Padronização e transformação dos dados para o formato **Parquet**.
- Criação de um banco analítico **DuckDB** consolidado (`database.duckdb`).

📁 Organização das pastas:
- `/data/raw/` → arquivos CSV/XLSX extraídos
- `/data/parquet/` → arquivos tratados e otimizados
- `/data/cnes/` e `/data/cidades_final/` → dados prontos para análise
- `/data/database.duckdb` → banco relacional pronto para consultas

---

## 📌 Fase 2 - Análise Exploratória de Dados (EDA)

🔍 Objetivo: investigar e compreender o comportamento dos dados.

- Estatísticas descritivas.
- Visualizações geográficas e demográficas.
- Análises comparativas entre variáveis socioeconômicas e infraestrutura de saúde.
- Identificação de padrões e anomalias entre municípios.

---

## 📌 Fase 3 - Ciência de Dados (PCA e Clusterização)

🧠 Objetivo: identificar grupos de municípios com características semelhantes.

- Aplicação de **PCA** (Análise de Componentes Principais) para redução de dimensionalidade.
- Execução de técnicas de **Clusterização** (ex.: K-Means, HDBSCAN).
- Interpretação dos grupos formados.
- Geração de **insights acadêmicos e políticos** sobre desigualdades regionais.

---

## 🚀 Tecnologias Utilizadas

- Python 3.11+
- [Polars](https://www.pola.rs/)
- [DuckDB](https://duckdb.org/)
- PyArrow
- Poetry
- Bibliotecas padrão do Python (`ftplib`, `zipfile`, `os`, `logging`)

---

## 🛠️ Como Executar

1. Clone o repositório:

   ```bash
   git clone https://github.com/guilhermecanfield/projeto_tcc.git
   cd projeto_tcc
   ```

2. Ative seu ambiente virtual:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows
   ```

3. Instale as dependências:

   ```bash
   poetry install
   ```

4. Execute o pipeline completo:

   ```bash
   python scripts/main.py
   ```

---

## 📋 Status Atual do Projeto

| Etapa | Status |
|:--|:--|
| 📦 Engenharia de Dados (Extração e Transformação) | ✅ Finalizada |
| 🔍 Análise Exploratória de Dados (EDA) | 🔜 Em breve |
| 🧠 Ciência de Dados (PCA e Clusterização) | 🔜 Em breve |

---

## 🎯 Objetivo Final

Fornecer subsídios acadêmicos robustos sobre as **disparidades regionais na oferta de serviços de saúde no Brasil**, apoiados em dados públicos e em técnicas modernas de análise e ciência de dados.