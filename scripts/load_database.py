# scripts/load_database.py

import duckdb
import os
from scripts.utils import configurar_logger, criar_pastas
from scripts.configs import DIRS, DB_PATH

logger = configurar_logger()

def carregar_parquets_para_duckdb():
    logger.info("Carregando tabelas do CNES no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    for arquivo in os.listdir(DIRS['CONCAT_CNES']):
        if arquivo.endswith('.parquet'):
            nome_tabela = arquivo.replace('_2022.parquet', '')
            caminho = os.path.join(DIRS['CONCAT_CNES'], arquivo)

            logger.info(f"Inserindo tabela {nome_tabela}...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE {nome_tabela} AS 
                SELECT * FROM read_parquet('{caminho}')
            """)
    conn.close()
    logger.info("Tabelas do CNES criadas e populadas com sucesso no DuckDB!")

def carregar_cidades_ibge_no_duckdb():
    logger.info("Carregando dados de tb_cidades_ibge_2022 no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    # Cria a tabela, caso não exista
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tb_cidades_ibge_2022 (
            nome TEXT,
            codigo_municipio TEXT,
            area_km2 DOUBLE,
            populacao_residente DOUBLE,
            densidade_demografica DOUBLE,
            escolarizacao_6_14 DOUBLE,
            idhm DOUBLE,
            mortalidade_infantil DOUBLE,
            total_receitas_brutas_realizadas DOUBLE,
            total_despesas_brutas_empenhadas DOUBLE,
            pib_per_capita DOUBLE,
            uf TEXT
        );
    """)

    # Faz o INSERT dos dados especificando as colunas
    conn.execute(f"""
        INSERT INTO tb_cidades_ibge_2022 (
            nome,
            codigo_municipio,
            area_km2,
            populacao_residente,
            densidade_demografica,
            escolarizacao_6_14,
            idhm,
            mortalidade_infantil,
            total_receitas_brutas_realizadas,
            total_despesas_brutas_empenhadas,
            pib_per_capita,
            uf
        )
        SELECT
            nome,
            SUBSTRING(codigo_municipio, 1, 6) AS codigo_municipio,
            area_km2,
            populacao_residente,
            densidade_demografica,
            escolarizacao_6_14,
            idhm,
            mortalidade_infantil,
            total_receitas_brutas_realizadas,
            total_despesas_brutas_empenhadas,
            pib_per_capita,
            uf
        FROM read_parquet('{DIRS["FINAL_CIDADES"]}/cidades_ibge_2022.parquet');
    """)

    conn.close()
    logger.info("Tabela tb_cidades_ibge_2022 criada e populada com sucesso no DuckDB!")

def carregar_mortalidade_no_duckdb():
    logger.info("Carregando dados de mortalidade no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    caminho = f'{DIRS["FINAL_MORTALIDADE"]}/mortalidade_2022.parquet'

    conn.execute("""
        CREATE OR REPLACE TABLE tb_mortalidade_2022 AS
        SELECT * FROM read_parquet(?)
    """, (caminho,))

    conn.close()
    logger.info("Tabela tb_mortalidade_2022 criada e populada com sucesso no DuckDB!")

def carregar_ibge_adicionais_no_duckdb():
    logger.info("Carregando tabelas adicionais do IBGE no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    for arquivo in os.listdir(DIRS['FINAL_IBGE']):
        if arquivo.endswith('.parquet'):
            nome_tabela = arquivo.replace('.parquet', '')
            caminho = f"{DIRS['FINAL_IBGE']}/{arquivo}"

            print(nome_tabela)

            logger.info(f"Inserindo tabela {nome_tabela}...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE {nome_tabela} AS 
                SELECT * FROM read_parquet('{caminho}')
            """)

    conn.close()
    logger.info("Tabelas adicionais do IBGE carregadas com sucesso no DuckDB!")

def carregar_tabela_principal():
    logger.info("Carregando tabela principal no DuckDB...")
    conn = duckdb.connect(DB_PATH)

    # Cria a tabela, caso não exista
    conn.execute("""
        CREATE OR REPLACE TABLE tabela_final AS
        WITH leitos AS(
            SELECT 
                rec.CO_UNIDADE,
                SUM(rec.QT_EXIST) leitos_existentes,
                SUM(rec.QT_SUS) leitos_sus
            FROM
                rlestabcomplementar rec
            WHERE
                rec.data_competencia = '2022-12-01'
                AND rec.CO_UNIDADE IN (
                    SELECT 
                        DISTINCT CO_UNIDADE 
                    FROM 
                        tbestabelecimento te
                    WHERE
                        te.CO_MOTIVO_DESAB = ''
                )
            GROUP BY ALL
        ),
        tabela_completa AS (
            SELECT
                tm.CO_SIGLA_ESTADO,
                tm.CO_MUNICIPIO,
                tm.NO_MUNICIPIO,
                te.TP_GESTAO,
                tte.DS_TIPO_ESTABELECIMENTO,
                ta.DS_ATIVIDADE,
                ta.DS_CONCEITO_ATIVIDADE,
                l.leitos_existentes,
                l.leitos_sus,
                ttu.DS_TIPO_UNIDADE,
                te.CO_UNIDADE,
                te.CO_CNES,
                te.NO_RAZAO_SOCIAL,
                te.NO_FANTASIA,
                te.TP_ESTAB_SEMPRE_ABERTO,
                te.CO_MOTIVO_DESAB
            FROM
                tbestabelecimento te
            LEFT JOIN
                tbmunicipio tm
                ON te.CO_MUNICIPIO_GESTOR = tm.CO_MUNICIPIO
            LEFT JOIN
                tbtipounidade ttu
                ON ttu.CO_TIPO_UNIDADE = te.TP_UNIDADE
            LEFT JOIN
                tbtipoestabelecimento tte
                ON te.CO_TIPO_ESTABELECIMENTO = tte.CO_TIPO_ESTABELECIMENTO
            LEFT JOIN
                tbatividade ta
                ON te.CO_ATIVIDADE_PRINCIPAL = ta.CO_ATIVIDADE
            LEFT JOIN
                leitos l
                ON l.CO_UNIDADE = te.CO_UNIDADE
            WHERE
                te.CO_MOTIVO_DESAB = ''
        ),
        obitos_por_estabelecimento AS (
            SELECT 
                o.CODESTAB, 
                COUNT(*) qtd_obitos 
            FROM 
                tb_mortalidade_2022 o
            GROUP BY 
                o.CODESTAB
        ),
        estabelecimentos_com_obitos AS (
            SELECT 
                tc.*,
                o.qtd_obitos 
            FROM 
                tabela_completa tc
            LEFT JOIN 
                obitos_por_estabelecimento o 
            ON 
                tc.CO_CNES = TRY_CAST(o.CODESTAB AS INTEGER)
        ),
        obitos_total AS (
            SELECT 
                tm.CO_MUNICIPIO codigo,
                COUNT(*) total_obitos
            FROM 
                tb_mortalidade_2022 o 
            JOIN
                tbmunicipio tm 
            ON
                o.CODMUNOCOR = tm.CO_MUNICIPIO
            WHERE
                tm.CO_MUNICIPIO IS NOT NULL
            GROUP BY
                ALL
        ),
        estabelecimentos_com_leito AS (
            SELECT
                tc.CO_MUNICIPIO,
                COUNT(*) quantidade_unidades_com_leito
            FROM
                tabela_completa tc
            WHERE
                tc.leitos_existentes > 0
            GROUP BY ALL
        ),
        estabelecimentos_por_municipio AS (
            SELECT 
                te.CO_MUNICIPIO_GESTOR,
                COUNT(*) quantidade_unidades
            FROM 
                tbestabelecimento te
            GROUP BY ALL
        ),
        socio_economicos AS (
            SELECT
                i.uf,
                i.codigo_municipio,
                i.nome,
                (i.populacao_residente / e.quantidade_unidades) hab_por_unidade,
                (e.quantidade_unidades / i.populacao_residente) unidades_por_hab,
                e.quantidade_unidades,
                i.area_km2 area_territorial,
                i.populacao_residente populacao,
                i.densidade_demografica,
                i.escolarizacao_6_14 matriculas_ensino_medio,
                i.idhm idh,
                i.total_receitas_brutas_realizadas total_receitas_brutas,
                i.total_despesas_brutas_empenhadas total_despesas_brutas,
                i.pib_per_capita,
                i.mortalidade_infantil
            FROM 
                tb_cidades_ibge_2022 i
            JOIN 
                estabelecimentos_por_municipio e
            ON 
                i.codigo_municipio = e.CO_MUNICIPIO_GESTOR
        ),
        profissionais AS (
            SELECT 
                tchs.CO_UNIDADE codigo_unidade,
                te.NO_FANTASIA nome_fantasia,
                tm.CO_MUNICIPIO codigo_municipio,
                tm.NO_MUNICIPIO municipio,
                tm.CO_SIGLA_ESTADO uf,
                tchs.CO_PROFISSIONAL_SUS codigo_profissional,
                tap.DS_ATIVIDADE_PROFISSIONAL atividade_profissional,
                tap.TP_CLASSIFICACAO_PROFISSIONAL classificacao_profissional,
                tap.TP_CBO_SAUDE cbo_saude,
                tchs.TP_SUS_NAO_SUS sus
            FROM tbcargahorariasus tchs 
            JOIN
                tbestabelecimento te
            ON
                te.CO_UNIDADE = tchs.CO_UNIDADE
            JOIN 
                tbatividadeprofissional tap 
            ON
                tap.CO_CBO = tchs.CO_CBO
            JOIN
                tbmunicipio tm
            ON
                tm.CO_MUNICIPIO = te.CO_MUNICIPIO_GESTOR
        ),
        medicos AS (
            SELECT 
                codigo_municipio, 
                COUNT(DISTINCT codigo_profissional) AS qtd_medicos
            FROM 
                profissionais
            WHERE 
                atividade_profissional ILIKE '%medic%'
            GROUP BY codigo_municipio
        ),
        enfermeiros AS (
            SELECT
                codigo_municipio,
                COUNT(DISTINCT codigo_profissional) AS qtd_enfermeiros
            FROM 
                profissionais
            WHERE 
                atividade_profissional ILIKE '%enfermei%'
            GROUP BY 
                codigo_municipio
        ),
        tabela_final AS (
        SELECT 
            se.uf,
            se.codigo_municipio,
            se.nome,
            se.populacao,
            se.idh,
            COUNT(DISTINCT CO_UNIDADE) qtd_unidades,
            ROUND(se.hab_por_unidade, 2) hab_por_unidade,
            COUNT(DISTINCT CO_UNIDADE) / MAX(se.populacao) * 1000 unidades_por_k_hab,
            ((m.qtd_medicos / se.populacao) * 1000) medicos_por_k_habitante,
            (se.populacao / m.qtd_medicos) habitantes_por_medico,
            ((e.qtd_enfermeiros / se.populacao) * 1000) enfermeiros_por_k_habitante,
            (se.populacao / e.qtd_enfermeiros) habitantes_por_enfermeiros,
            SUM(leitos_existentes) leitos_existentes,
            ecl.quantidade_unidades_com_leito,
            ((SUM(ecl.quantidade_unidades_com_leito) / SUM(se.populacao)) * 1000) quantidade_unidades_com_leito_por_k_hab,
            ROUND(SUM(leitos_existentes) / COUNT(DISTINCT CO_UNIDADE), 2) total_leitos_unidade,
            (SUM(leitos_existentes) / MAX(se.populacao) * 1000) leitos_por_k_hab,
            SUM(leitos_sus) leitos_sus,
            ROUND(SUM(leitos_sus) / COUNT(DISTINCT CO_UNIDADE), 2) total_leitos_sus_unidade,
            (SUM(leitos_sus) / MAX(se.populacao) * 1000) leitos_sus_por_k_hab,
            ROUND(SUM(qtd_obitos) / COUNT(DISTINCT CO_UNIDADE), 2) obitos_por_unidade,
            SUM(qtd_obitos) obitos_em_estabelecimentos,
            ot.total_obitos total_obitos,
            ROUND((ot.total_obitos / se.populacao) * 1000, 2) taxa_mortalidade_geral,
            se.mortalidade_infantil,
            se.area_territorial,
            se.densidade_demografica,
            se.matriculas_ensino_medio,
            se.total_receitas_brutas,
            se.total_despesas_brutas,
            se.pib_per_capita,
            a.total taxa_de_alfabetizados,
            (100 - a.total) taxa_de_nao_alfabetizados,
            cdl.pct_coletado pct_coleta_lixo,
            lre.sim pct_com_rede_esgoto,
            lre.nao pct_sem_rede_esgoto,
            mae.total media_anos_estudo_geral,
            mae."11_14" media_anos_estudo_11_14,
            mae."15_17" media_anos_estudo_15_17,
            mae."18_24" media_anos_estudo_18_24,
            mae."25_mais" media_anos_estudo_25_mais,
            ni.sem_instrucao_fundamental_incompleto taxa_adultos_sem_instrucao,
            ni.fundamental_completo_medio_incompleto taxa_adultos_fundamental_completo,
            ni.medio_completo_superior_incompleto taxa_adultos_medio_completo,
            ni.superior_completo taxa_adultos_superior_completo,
            tde.pct_60_mais pct_idoso,
            prf.total / se.populacao taxa_pop_residente_favela,
            tsd.urbana taxa_populacao_urbana,
            tsd.rural taxa_populacao_rural,
            tfe.total taxa_freq_escolar
        FROM 
            estabelecimentos_com_obitos eco
        LEFT JOIN
            socio_economicos se
        ON
            se.codigo_municipio = eco.CO_MUNICIPIO
        LEFT JOIN
            obitos_total ot
        ON
            ot.codigo = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_de_alfabetizacao a
        ON
            SUBSTRING(a.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_coleta_lixo cdl
        ON
            SUBSTRING(cdl.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_rede_esgoto lre 
        ON
            SUBSTRING(lre.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            anos_de_estudo mae 
        ON
            SUBSTRING(mae.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_populacao_nivel_instrucao ni 
        ON
            SUBSTRING(ni.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            pop_res_favela prf
        ON
            SUBSTRING(prf.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_situacao_domicilio tsd 
        ON
            SUBSTRING(tsd.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_frequencia_escolar tfe
        ON
            SUBSTRING(tfe.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            taxa_distribuicao_etaria tde
        ON
            SUBSTRING(tde.codigo, 1, 6) = eco.CO_MUNICIPIO
        LEFT JOIN
            medicos m
        ON
            m.codigo_municipio = eco.CO_MUNICIPIO
        LEFT JOIN
            enfermeiros e
        ON
            e.codigo_municipio = eco.CO_MUNICIPIO
        LEFT JOIN
            estabelecimentos_com_leito ecl
        ON 
            ecl.CO_MUNICIPIO = eco.CO_MUNICIPIO
        GROUP BY ALL
        )
        SELECT * FROM tabela_final
    """)

    logger.info("Tabela Final criada e populada com sucesso no DuckDB!")

    # Garante que o diretório 'tabela_final' existe
    os.makedirs(DIRS['TABELA_FINAL'], exist_ok=True)

    # Lê a tabela final do DuckDB como DataFrame
    logger.info("Exportando tabela final para Parquet...")
    df = conn.execute("SELECT * FROM tabela_final").fetchdf()  # fetchdf() retorna um pandas DataFrame

    # Salva o DataFrame em arquivo Parquet
    df.to_parquet(f"{DIRS['TABELA_FINAL']}/tabela_final.parquet", index=False)

    logger.info(f"Arquivo Parquet salvo com sucesso em '{DIRS['TABELA_FINAL']}/tabela_final.parquet'!")
    conn.close()