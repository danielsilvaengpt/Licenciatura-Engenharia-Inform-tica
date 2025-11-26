import os
import sys
import pyodbc
import csv
from datetime import datetime

# ---------------------------------------------------------
# 1. Configuração
# ---------------------------------------------------------

CSV_PATH = os.getenv("FP7", r"dados_atualizado.csv")  # Nome do ficheiro CSV
MSSQL_HOST = os.getenv("MSSQL_HOST", "127.0.0.1")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER = os.getenv("MSSQL_USER", "sa")
MSSQL_PWD = os.getenv("MSSQL_PWD", "root2025")
MSSQL_DB = os.getenv("MSSQL_DB", "TP_G2_Viagens")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")


# ---------------------------------------------------------
# 2. Ligações
# ---------------------------------------------------------

def get_mssql_conn():
    """Liga ao SQL Server via pyodbc (schema padrão: dbo)."""
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DB};UID={MSSQL_USER};PWD={MSSQL_PWD};"
        f"TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str)


# ---------------------------------------------------------
# 3. FUNÇÕES AUXILIARES DE TRANSFORMAÇÃO
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


# ---------------------------------------------------------
# 4. FUNÇÕES get_or_create (INJEÇÃO DO CÓDIGO DO ETL PRINCIPAL)
# ---------------------------------------------------------

def get_or_create_dim_tempo(cur, data_ref):
    """Gere a dimensão Tempo (IDENTITY). Procura pela Chave Natural (data_completa) e insere se não existir."""
    cur.execute("SELECT idtempo FROM tempo WHERE data_completa = ?", (data_ref,))
    row = cur.fetchone()
    if row: return row[0]

    ano, mes = data_ref.year, data_ref.month
    trimestre = (mes - 1) // 3 + 1
    semestre = 1 if mes <= 6 else 2

    try:
        cur.execute("""
                    INSERT INTO tempo (data_completa, ano, mes, semestre, trimestre)
                    VALUES (?, ?, ?, ?, ?);
                    """, (data_ref, ano, mes, semestre, trimestre))
        cur.execute("SELECT SCOPE_IDENTITY();")
        row = cur.fetchone()
        if row:
            return int(row[0])
        else:
            raise RuntimeError("Falha ao obter IdTempo após INSERT.")
    except Exception as e:
        cur.execute("SELECT idtempo FROM tempo WHERE data_completa = ?", (data_ref,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_localizacao(cur, localizacao_data):
    """Gere a dimensão Localização (IDENTITY). Procura por Cidade e País (Chave Natural do CSV)."""
    cidade = localizacao_data["cidade"]
    pais = localizacao_data["pais"]

    # 1. Procura pela Chave Natural Composta (Cidade + País)
    cur.execute("SELECT idlocalizacao FROM localizacao WHERE cidade = ? AND pais = ?", (cidade, pais))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Insere, omitindo a PK (idlocalizacao). Usamos um ID de origem fictício.
    try:
        id_origem_ficticio = f"CSV_{cidade}_{pais}".replace(' ', '_')

        cur.execute("""
                    INSERT INTO localizacao (cidade, pais, localizacao_id_origem)
                    VALUES (?, ?, ?);
                    """, (cidade, pais, id_origem_ficticio))

        # 3. Obtém o novo ID gerado
        cur.execute("SELECT SCOPE_IDENTITY();")
        row = cur.fetchone()
        if row:
            return int(row[0])
        else:
            raise RuntimeError("Falha ao obter IdLocalizacao após INSERT.")
    except Exception as e:
        cur.execute("SELECT idlocalizacao FROM localizacao WHERE cidade = ? AND pais = ?", (cidade, pais))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_condutor(cur, condutor_data):
    """Gere a dimensão Condutor (IDENTITY). Procura por Nome e Certificação (Chave Natural do CSV)."""
    nome = condutor_data["nome"]
    certificacao = condutor_data["certificacao"]

    # 1. Procura pela Chave Natural Composta (Nome + Certificação)
    cur.execute("SELECT idcondutor FROM condutor WHERE nome = ? AND certificacao = ?", (nome, certificacao))
    row = cur.fetchone()
    if row: return row[0]

    # 2. Insere, omitindo a PK (idcondutor). Usamos um ID de origem fictício.
    try:
        id_origem_ficticio = f"CSV_{nome.replace(' ', '_')}_{certificacao}"

        cur.execute("""
                    INSERT INTO condutor (nome, idade, certificacao)
                    VALUES (?, ?, ?, ?, ?);
                    """, (
                        nome,
                        condutor_data["idade"],
                        certificacao,
                        condutor_data["sexo"],
                        id_origem_ficticio
                    ))
        # 3. Obtém o novo ID gerado
        cur.execute("SELECT SCOPE_IDENTITY();")
        row = cur.fetchone()
        if row:
            return int(row[0])
        else:
            raise RuntimeError("Falha ao obter IdCondutor após INSERT.")
    except Exception as e:
        cur.execute("SELECT idcondutor FROM condutor WHERE nome = ? AND certificacao = ?", (nome, certificacao))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_tipo_viagem(cur, tipo_viagem_text):
    """Gere a dimensão Tipo Viagem (IDENTITY - SK)."""
    cur.execute("SELECT idtipoviagem FROM tipo_viagem WHERE tipo = ?", (tipo_viagem_text,))
    row = cur.fetchone()
    if row: return row[0]

    try:
        cur.execute("INSERT INTO tipo_viagem (tipo) VALUES (?);", (tipo_viagem_text,))
        cur.execute("SELECT SCOPE_IDENTITY();")
        row = cur.fetchone()
        if row:
            return int(row[0])
        else:
            raise RuntimeError("Falha ao obter IdTipoViagem após INSERT.")
    except Exception as e:
        cur.execute("SELECT idtipoviagem FROM tipo_viagem WHERE tipo = ?", (tipo_viagem_text,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_classeduracao(cur, duracao_dias):
    """Gere a dimensão Classe Duracao (IDENTITY - SK)."""
    nome_classe = mapeia_duracao_para_texto(duracao_dias)
    cur.execute("SELECT idclasseduracao FROM classeduracao WHERE duracao = ?", (nome_classe,))
    row = cur.fetchone()
    if row: return row[0]

    try:
        cur.execute("INSERT INTO classeduracao (duracao) VALUES (?);", (nome_classe,))
        cur.execute("SELECT SCOPE_IDENTITY();")
        row = cur.fetchone()
        if row:
            return int(row[0])
        else:
            raise RuntimeError("Falha ao obter IdClasseDuracao após INSERT.")
    except Exception as e:
        cur.execute("SELECT idclasseduracao FROM classeduracao WHERE duracao = ?", (nome_classe,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


# ---------------------------------------------------------
# FUNÇÕES DUMMY PARA O SNOWFLAKE: EMPRESA/BARCO
# Como o CSV não tem IDs de origem nem nome da empresa, temos que 
# usar o Nome do Barco como Chave Natural para lookup.
# ---------------------------------------------------------

def lookup_dim_empresabarco_by_barco_name(cur, nome_barco):
    """
    Função DUMMY: Tenta encontrar a FK da Empresa através do Barco (já carregado pelo MySQL).
    Se o Barco não foi carregado pelo ETL MySQL, isto falha.
    """
    cur.execute("""
                SELECT eb.idempresa_barco
                FROM barco b
                         JOIN empresabarco eb ON b.empresabarco_idempresa_barco = eb.idempresa_barco
                WHERE b.nome = ?
                """, (nome_barco,))
    row = cur.fetchone()
    if row: return row[0]
    # Se falhar, devolve um ID fictício para o "Barco Desconhecido" (ID=1)
    # ATENÇÃO: É NECESSÁRIO CRIAR UM REGISTO EMPRESA/BARCO COM ID=1 NO DDL
    return 1


def lookup_dim_barco(cur, nome_barco):
    """
    Função DUMMY: Tenta encontrar a SK do Barco pelo nome.
    Se o Barco não foi carregado pelo ETL MySQL, isto falha.
    """
    cur.execute("SELECT idbarco FROM barco WHERE nome = ?", (nome_barco,))
    row = cur.fetchone()
    if row: return row[0]
    # Se falhar, devolve um ID fictício para o "Barco Desconhecido" (ID=1)
    # ATENÇÃO: É NECESSÁRIO CRIAR UM REGISTO BARCO COM ID=1 NO DDL
    return 1


# ---------------------------------------------------------
# 5. FUNÇÃO PRINCIPAL
# ---------------------------------------------------------

def main_csv_processor():
    """
    Processa o ficheiro CSV e insere as Dimensões e a Tabela de Factos.
    Insere 0 para os factos agregados (containers, peso, TEU) que faltam no CSV.
    """
    print("--- 1. Iniciando ETL de Dimensões e Factos via CSV ---")
    sqlsrv_conn = get_mssql_conn()

    try:
        sqlsrv_cur = sqlsrv_conn.cursor()

        # Leitura e conversão inicial do CSV
        csv_data = []
        try:
            # O ficheiro do utilizador tem delimitador ';'
            with open(CSV_PATH, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row_csv in reader:
                    csv_data.append(row_csv)
        except FileNotFoundError:
            print(f"ERRO: ficheiro CSV não encontrado: {CSV_PATH}")
            sys.exit(1)

        print(f"Total de {len(csv_data)} linhas lidas do CSV.")

        linhas_processadas = 0

        for r_csv in csv_data:

            # --- TRANSFORMAÇÃO DE CAMPOS BRUTOS DO CSV ---
            try:
                # Conversão de Data e Cálculo da Duração
                data_chegada = datetime.strptime(r_csv['datachegada'], '%d/%m/%Y').date()
                data_partida = datetime.strptime(r_csv['datapartida'], '%d/%m/%Y').date()
                duracao = (data_chegada - data_partida).days

                # Conversão da Taxa (A coluna 'taxa' está com ',' como separador decimal)
                taxa_eur = float(r_csv['taxa'].replace(',', '.'))


                # Prepara dados para Condutor
                data_condutor = {
                    "nome": r_csv['nomecondutor'],
                    "idade": int(r_csv['idadecondutor']),
                    "certificacao": r_csv['certificacao']
                }

                # Prepara dados para Localização
                data_localizacao = {
                    "pais": r_csv['pais_origem'],
                    "cidade": r_csv['cidade_origem'],
                }

                # --- OBTENÇÃO DAS CHAVES SUBSTITUTAS (SKs) ---

                # As SKs do Barco e Empresa são encontradas por lookup (assumindo pré-carregamento)
                id_barco = lookup_dim_barco(sqlsrv_cur, r_csv['nomebarco'])

                # Se o Barco foi encontrado, a empresa deve ser procurada a partir dele.
                # Como o lookup_dim_barco devolve o SK, usamos o nome do barco para obter o SK da empresa.
                # Devido à limitação do CSV, vamos usar a função DUMMY.
                id_empresa_barco = lookup_dim_empresabarco_by_barco_name(sqlsrv_cur, r_csv['nomebarco'])

                # Chaves restantes
                id_tempo = get_or_create_dim_tempo(sqlsrv_cur, data_chegada)
                id_condutor = get_or_create_dim_condutor(sqlsrv_cur, data_condutor)
                id_localizacao = get_or_create_dim_localizacao(sqlsrv_cur, data_localizacao)
                id_tipo_viagem = get_or_create_dim_tipo_viagem(sqlsrv_cur, r_csv['tipobarco'])
                id_classe_duracao = get_or_create_dim_classeduracao(sqlsrv_cur, duracao)

                # --- INSERÇÃO NA TABELA DE FACTOS (VIAGENS) ---

                # NOTA: O CSV só tem taxa. Os outros factos são preenchidos com 0.
                sqlsrv_cur.execute("""
                                   INSERT INTO viagens (viagem_id_origem, duracaoviagem, totaltaxas, numerocontentores,
                                                        pesototalcontentores, teutotal, classeduracao_idclasseduracao,
                                                        localizacao_idlocalizacao, tipo_viagem_idtipoviagem,
                                                        condutor_idcondutor, barco_idbarco, tempo_idtempo)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                   """, (
                                       r_csv['idviagem'],  # Chave Natural da Viagem (ID de Origem)
                                       duracao,  # Facto: Duração
                                       taxa_eur,  # Facto: Receita (Convertida de USD/CSV)
                                       0,  # Facto: Contentores (0 - Faltam dados no CSV)
                                       0,  # Facto: Peso (0 - Faltam dados no CSV)
                                       0,  # Facto: TEU (0 - Faltam dados no CSV)
                                       id_classe_duracao,  # FK Classe Duracao
                                       id_localizacao,  # FK Localizacao
                                       id_tipo_viagem,  # FK Tipo Viagem
                                       id_condutor,  # FK Condutor
                                       id_barco,  # FK Barco (Lookup SK)
                                       id_tempo,  # FK Tempo
                                   ))

                linhas_processadas += 1
                if linhas_processadas % 100 == 0:
                    sqlsrv_conn.commit()
                    print(f"... {linhas_processadas} linhas de factos (CSV) processadas...")

            except ValueError as ve:
                print(f"Erro de conversão de tipo na linha {r_csv.get('idviagem', 'N/A')}. Erro: {ve}")
                continue
            except Exception as e:
                print(f"Erro inesperado ao processar linha {r_csv.get('idviagem', 'N/A')}: {e}")
                continue

        sqlsrv_conn.commit()
        print(f"ETL CSV concluído. Total de Factos inseridos: {linhas_processadas}")

    finally:
        if sqlsrv_conn:
            sqlsrv_conn.close()


if __name__ == "__main__":
    main_csv_processor()