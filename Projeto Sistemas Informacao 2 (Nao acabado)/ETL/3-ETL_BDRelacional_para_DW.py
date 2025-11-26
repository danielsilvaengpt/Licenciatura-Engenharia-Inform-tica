import os
import sys
import mysql.connector as mysql
import pyodbc
from datetime import datetime

# ---------------------------------------------------------
# Variaveis de ligação aos SGBD (Mantidas como no original)
# ---------------------------------------------------------
# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PWD = os.getenv("MYSQL_PWD", "root2025")
MYSQL_DB = os.getenv("MYSQL_DB", "TP_G2")

# SQL Server
MSSQL_HOST = os.getenv("MSSQL_HOST", "127.0.0.1")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER = os.getenv("MSSQL_USER", "sa")
MSSQL_PWD = os.getenv("MSSQL_PWD", "root2025")
MSSQL_DB = os.getenv("MSSQL_DB", "TP_G2_Viagens")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")


# ---------------------------------------------------------
# Ligações ao SGBD
# ---------------------------------------------------------

def get_mysql_conn():
    """Liga ao MySQL via mysql-connector-python."""
    return mysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PWD,
        database=MYSQL_DB, autocommit=True, charset="utf8mb4"
    )


def get_mssql_conn():
    """Liga ao SQL Server via pyodbc (schema padrão: dbo)."""
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DB};UID={MSSQL_USER};PWD={MSSQL_PWD};"
        f"TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str)


# ---------------------------------------------------------
# FUNÇÕES AUXILIARES DE TRANSFORMAÇÃO (Mantidas/Adaptadas)
# ---------------------------------------------------------

def mapeia_duracao_para_texto(duracao_dias):
    """Função para classificar a duração bruta (em dias) em uma classe textual."""
    if duracao_dias <= 7:
        return "0-7"
    elif duracao_dias <= 15:
        return "8-15"
    elif duracao_dias <= 30:
        return "16-30"
    elif duracao_dias <= 60:
        return "31-60"
    else:
        return "60+"


def get_next_id(cur, table_name, id_column):
    """Obtém o próximo ID sequencial para uma tabela no DW, assumindo PK não-IDENTITY."""
    # Usar f-string para o nome da tabela e coluna, pois não são dados de input do usuário
    cur.execute(f"SELECT MAX({id_column}) FROM {table_name}")
    max_id = cur.fetchone()[0]
    return (max_id or 0) + 1


# ---------------------------------------------------------
# FUNÇÕES get_or_create (Refatoradas para Chaves Explícitas)
# ---------------------------------------------------------

def get_or_create_dim_tempo(cur, data_ref):
    """
    Gere a dimensão Tempo (PK Explícita).
    Procura pela Chave Natural (data_completa) e insere se não existir.
    """

    # 1. Procura pela Chave Natural (Data)
    cur.execute("SELECT idtempo FROM tempo WHERE data_completa = ?", (data_ref,))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Se não existe, calcular os atributos derivados
    ano, mes = data_ref.year, data_ref.month
    trimestre = (mes - 1) // 3 + 1
    semestre = 1 if mes <= 6 else 2

    # 3. Gerar o novo ID
    next_id = get_next_id(cur, "tempo", "idtempo")

    # 4. Insere, usando a PK explícita
    try:
        cur.execute("""
                    INSERT INTO tempo (idtempo, data_completa, ano, mes, semestre, trimestre)
                    VALUES (?, ?, ?, ?, ?, ?);
                    """, (next_id, data_ref, ano, mes, semestre, trimestre))
        return next_id
    except Exception as e:
        # Fallback (procura novamente em caso de concorrência)
        cur.execute("SELECT idtempo FROM tempo WHERE data_completa = ?", (data_ref,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_localizacao(cur, localizacao_data):
    """Gere a dimensão Localização (PK Explícita).
       Usa 'pais' e 'cidade' como Chave Natural.
    """

    # 1. Procura pela Chave Natural (País + Cidade)
    pais = localizacao_data["pais"]
    cidade = localizacao_data["cidade"]
    cur.execute("SELECT idlocalizacao FROM localizacao WHERE pais = ? AND cidade = ?", (pais, cidade))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Gerar o novo ID
    next_id = get_next_id(cur, "localizacao", "idlocalizacao")

    # 3. Insere, usando a PK explícita
    try:
        cur.execute("""
                    INSERT INTO localizacao (idlocalizacao, pais, cidade)
                    VALUES (?, ?, ?);
                    """, (next_id, pais, cidade))
        return next_id
    except Exception as e:
        # Fallback (procura novamente em caso de concorrência)
        cur.execute("SELECT idlocalizacao FROM localizacao WHERE pais = ? AND cidade = ?", (pais, cidade))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_condutor(cur, condutor_data):
    """Gere a dimensão Condutor (PK Explícita).
       Usa a combinação (nome, certificacao) como Chave Natural.
    """

    nome = condutor_data["nome"]
    certificacao = condutor_data["certificacao"]

    # 1. Procura pela Chave Natural (Nome + Certificação)
    cur.execute("SELECT idcondutor FROM condutor WHERE nome = ? AND certificacao = ?", (nome, certificacao))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Gerar o novo ID
    next_id = get_next_id(cur, "condutor", "idcondutor")

    # 3. Insere, usando a PK explícita (CORRIGIDO)
    try:
        cur.execute("""
                    INSERT INTO condutor (idcondutor, nome, idade, certificacao)
                    VALUES (?, ?, ?, ?);
                    """, (next_id, nome, condutor_data["idade"], certificacao))
        return next_id
    except Exception as e:
        # Fallback (procura novamente em caso de concorrência)
        cur.execute("SELECT idcondutor FROM condutor WHERE nome = ? AND certificacao = ?", (nome, certificacao))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_tipo_viagem(cur, tipo_viagem_text):
    """Gere a dimensão Tipo Viagem (PK Explícita)."""
    cur.execute("SELECT idtipoviagem FROM tipo_viagem WHERE tipo = ?", (tipo_viagem_text,))
    row = cur.fetchone()
    if row: return row[0]

    # 1. Gerar o novo ID
    next_id = get_next_id(cur, "tipo_viagem", "idtipoviagem")

    try:
        cur.execute("INSERT INTO tipo_viagem (idtipoviagem, tipo) VALUES (?, ?);", (next_id, tipo_viagem_text,))
        return next_id
    except Exception as e:
        cur.execute("SELECT idtipoviagem FROM tipo_viagem WHERE tipo = ?", (tipo_viagem_text,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_classeduracao(cur, duracao_dias):
    """Gere a dimensão Classe Duracao (PK Explícita)."""
    nome_classe = mapeia_duracao_para_texto(duracao_dias)
    cur.execute("SELECT idclasseduracao FROM classeduracao WHERE duracao = ?", (nome_classe,))
    row = cur.fetchone()
    if row: return row[0]

    # 1. Gerar o novo ID
    next_id = get_next_id(cur, "classeduracao", "idclasseduracao")

    try:
        cur.execute("INSERT INTO classeduracao (idclasseduracao, duracao) VALUES (?, ?);", (next_id, nome_classe,))
        return next_id
    except Exception as e:
        cur.execute("SELECT idclasseduracao FROM classeduracao WHERE duracao = ?", (nome_classe,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_empresabarco(cur, empresa_data):
    """Gere a dimensão EmpresaBarco (PK Explícita).
       Usa 'nome' e 'pais' como Chave Natural.
    """

    nome = empresa_data["nomeempresabarco"]
    pais = empresa_data["paisempresabarco"]

    # 1. Procura pela Chave Natural (Nome + País)
    cur.execute("SELECT idempresa_barco FROM empresabarco WHERE nome = ? AND pais = ?", (nome, pais))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Gerar o novo ID
    next_id = get_next_id(cur, "empresabarco", "idempresa_barco")

    # 3. Insere, usando a PK explícita
    try:
        cur.execute("""
                    INSERT INTO empresabarco (idempresa_barco, nome, pais)
                    VALUES (?, ?, ?);
                    """, (next_id, nome, pais))
        return next_id
    except Exception as e:
        # Fallback (procura novamente em caso de concorrência)
        cur.execute("SELECT idempresa_barco FROM empresabarco WHERE nome = ? AND pais = ?", (nome, pais))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_barco(cur, barco_data):
    """Gere a dimensão Barco (PK Explícita).
       Usa 'nome' e 'tamanho' como Chave Natural.
    """

    nome = barco_data["nomebarco"]
    tamanho = barco_data["tamanho"]

    # 1. Procura pela Chave Natural (Nome + Tamanho)
    cur.execute("SELECT idbarco FROM barco WHERE nome = ? AND tamanho = ?", (nome, tamanho))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Gerar o novo ID
    next_id = get_next_id(cur, "barco", "idbarco")

    # 3. Insere, usando a PK explícita
    try:
        # ATENÇÃO: A FK 'empresabarco_idempresa_barco' deve ser a CHAVE SUBSTITUTA,
        # que já foi obtida no loop principal (id_empresa_barco).
        cur.execute("""
                    INSERT INTO barco (idbarco, nome, tamanho, tipo, capacidade, empresabarco_idempresa_barco)
                    VALUES (?, ?, ?, ?, ?, ?);
                    """, (next_id, nome, tamanho, barco_data["tipobarco"],
                          barco_data["capacidadeteu"], barco_data["empresabarco_idempresabarco"]))
        return next_id
    except Exception as e:
        # Fallback (procura novamente em caso de concorrência)
        cur.execute("SELECT idbarco FROM barco WHERE nome = ? AND tamanho = ?", (nome, tamanho))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


# ---------------------------------------------------------
# FUNÇÃO PRINCIPAL ETL
# ---------------------------------------------------------

def main():
    print("1 - Ligação ao MySQL (Origem)")
    mysql_conn = get_mysql_conn()
    print("2 - Ligação ao MsSQL (Data Warehouse)")
    sqlsrv_conn = get_mssql_conn()

    try:
        mysql_cur = mysql_conn.cursor(dictionary=True)
        sqlsrv_cur = sqlsrv_conn.cursor()

        # [FASE 1: EXTRAÇÃO COMPLEXA DO MYSQL - QUERY CORRIGIDA]

        query_mysql_viagens = f"""
            SELECT
                v.idviagem,
                v.datapartida AS data_partida,
                v.datachegada AS data_chegada,
                v.tipoviagem,
                v.localizacao_idlocalizacao as id_localizacao_origem,
                v.condutor_idcondutor,
                v.barco_idbarco,

                l.pais as pais_origem,
                l.cidade as cidade_origem,

                c.nomecondutor,
                c.idadecondutor AS idade,
                c.certificacao,

                b.nomebarco,
                b.tamanhobarco AS tamanho,
                b.tipobarco,
                b.capacidadeteu,
                b.empresabarco_idempresabarco,

                eb.nomeempresabarco,
                eb.paisempresabarco,

                -- MÉTricas (Factos)
                COALESCE(SUM(t.valor), 0) AS totaltaxas_eur,
                COALESCE(COUNT(ct.idcontentor), 0) AS num_contentores_total,
                COALESCE(SUM(ct.pesocontentor), 0) AS peso_total_kg,
                COALESCE(SUM(CAST(ct.tamanho AS DECIMAL(10,2)) / 20.0), 0) AS teu_total_calc

            FROM viagem v
            JOIN localizacao l ON v.localizacao_idlocalizacao = l.idlocalizacao
            JOIN condutor c ON v.condutor_idcondutor = c.idcondutor
            JOIN barco b ON v.barco_idbarco = b.idbarco
            JOIN empresabarco eb ON b.empresabarco_idempresabarco = eb.idempresabarco
            LEFT JOIN taxas t ON v.idviagem = t.viagem_idviagem
            LEFT JOIN contentores ct ON v.idviagem = ct.viagem_idviagem

            -- FILTROS DE NEGÓCIO (Requisito do Enunciado)
            WHERE v.status = 'concluida' 
            -- CORRIGIDO: Usado IN para evitar o erro "Subquery returns more than 1 row"
            AND v.localizacao_idlocalizacao1 IN (
                SELECT idlocalizacao FROM localizacao WHERE lower(cidade) = "figfoz" AND lower(pais) = "portugal" 
            )

            GROUP BY 
                v.idviagem, v.datapartida, v.datachegada, v.tipoviagem, v.localizacao_idlocalizacao, 
                v.condutor_idcondutor, v.barco_idbarco, l.pais, l.cidade, c.nomecondutor, c.idadecondutor, 
                c.certificacao, b.nomebarco, b.tamanhobarco, b.tipobarco, b.capacidadeteu, 
                b.empresabarco_idempresabarco, eb.nomeempresabarco, eb.paisempresabarco
            ORDER BY v.datachegada;
        """
        mysql_cur.execute(query_mysql_viagens)
        rows_mysql = mysql_cur.fetchall()
        print(f"Registos lidos do MySQL (Viagens): {len(rows_mysql)}")

        # ----------------------------------------------------------------------
        # FASE 2: TRANSFORMAÇÃO E CARGA (Loop de Viagens)
        # ----------------------------------------------------------------------

        print("Iniciando Transformação e Carga (Factos)...")
        linhas_processadas = 0

        for idx, r_mysql in enumerate(rows_mysql, start=1):

            # B. TRANSFORMAÇÃO E OBTENÇÃO DE IDs (DIMENSÕES)

            # 1) Condutor (PK Explícita)
            data_condutor = {
                "idcondutor": r_mysql["condutor_idcondutor"],  # ID de Origem (usado aqui como dado para lookup)
                "nome": r_mysql["nomecondutor"],
                "idade": int(r_mysql["idade"]),
                "certificacao": r_mysql["certificacao"],
            }
            id_condutor = get_or_create_dim_condutor(sqlsrv_cur, data_condutor)

            # 2) Empresa Barco (PK Explícita - Pai do Snowflake)
            data_empresa = {
                "idempresabarco": r_mysql["empresabarco_idempresabarco"],  # ID de Origem
                "nomeempresabarco": r_mysql["nomeempresabarco"],
                "paisempresabarco": r_mysql["paisempresabarco"],
            }
            id_empresa_barco = get_or_create_dim_empresabarco(sqlsrv_cur, data_empresa)

            # 3) Barco (PK Explícita - Filho do Snowflake)
            data_barco = {
                "idbarco": r_mysql["barco_idbarco"],  # ID de Origem
                "nomebarco": r_mysql["nomebarco"],
                "tamanho": r_mysql["tamanho"],
                "tipobarco": r_mysql["tipobarco"],
                "capacidadeteu": int(r_mysql["capacidadeteu"]),
                "empresabarco_idempresabarco": id_empresa_barco  # FK SUBSTITUTA OBTIDA NO PASSO 2
            }
            id_barco = get_or_create_dim_barco(sqlsrv_cur, data_barco)

            # 4) Localização (Origem - PK Explícita)
            data_localizacao = {
                "idlocalizacao": r_mysql["id_localizacao_origem"],  # ID de Origem
                "pais": r_mysql["pais_origem"],
                "cidade": r_mysql["cidade_origem"],
            }
            id_localizacao = get_or_create_dim_localizacao(sqlsrv_cur, data_localizacao)

            # 5) Tipo Viagem (PK Explícita)
            id_tipo_viagem = get_or_create_dim_tipo_viagem(sqlsrv_cur, r_mysql["tipoviagem"])

            # 6) Tempo (PK Explícita)
            data_chegada = r_mysql["data_chegada"]
            id_tempo = get_or_create_dim_tempo(sqlsrv_cur, data_chegada)

            # C. CÁLCULOS E TRANSFORMAÇÕES PARA FACTOS

            duracao = (data_chegada - r_mysql["data_partida"]).days
            id_classe_duracao = get_or_create_dim_classeduracao(sqlsrv_cur, duracao)

            taxa_eur = float(r_mysql['totaltaxas_eur'])

            # D. INSERÇÃO NA TABELA DE FACTOS (viagens)
            # OMITIMOS a PK 'idviagens' (IDENTITY)
            sqlsrv_cur.execute("""
                               INSERT INTO viagens (idviagens, duracaoviagem, totaltaxas, numerocontentores,
                                                    pesototalcontentores, teutotal, classeduracao_idclasseduracao,
                                                    localizacao_idlocalizacao, tipo_viagem_idtipoviagem,
                                                    condutor_idcondutor, barco_idbarco, tempo_idtempo)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                               """, (
                                   (linhas_processadas + 1),  # Chave Natural da Viagem
                                   duracao,  # Facto
                                   taxa_eur,  # Facto (EUR)
                                   int(r_mysql['num_contentores_total']),  # Facto
                                   int(r_mysql['peso_total_kg']),  # Facto
                                   int(r_mysql['teu_total_calc']),  # Facto
                                   id_classe_duracao,  # FK Substituta
                                   id_localizacao,  # FK Substituta
                                   id_tipo_viagem,  # FK Substituta
                                   id_condutor,  # FK Substituta
                                   id_barco,  # FK Substituta
                                   id_tempo,  # FK Substituta
                               ))

            linhas_processadas += 1
            if linhas_processadas % 100 == 0:
                sqlsrv_conn.commit()
                print(f"... {linhas_processadas} registos de Factos processados")

        sqlsrv_conn.commit()
        print(f"ETL concluído. Total de Viagens Carregadas: {linhas_processadas}")
    finally:
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
        if sqlsrv_conn:
            sqlsrv_conn.close()


if __name__ == "__main__":
    main()