# Carga de dados e Analytics

# Imports
import csv
import pandas as pd
from cassandra.cluster import Cluster

# Este arquivo não deve ser executado diretamente. Colocamos uma mensagem para lembrar o Engenheiro de Dados.
if __name__ == '__main__':
    print("Este arquivo não deve ser executado diretamente. Execute: etl_app.py")

# Arquivo de dados que será carregado no Apache Cassandra
file_name = 'resultado/dataset_completo.csv'

# Função para criar o cluster Cassandra
def conecta_cluster():

    # Cria a conexão ao cluster Cassandra
    cluster = Cluster(['localhost'])

    # Estabelece a conexão
    session = cluster.connect()

    # Cria a keyspace
    session.execute("CREATE KEYSPACE IF NOT EXISTS projeto1 WITH REPLICATION = "
                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    # Define o tipo da keyspace
    session.set_keyspace('projeto1')

    # Executa os métodos de Analytics
    pipeline_analytics_1(session)
    pipeline_analytics_2(session)
    pipeline_analytics_3(session)

    # Deleta as tabelas após o Analytics
    drop_tables(session)

    # Desliga os recursos
    session.shutdown()
    cluster.shutdown()

    print("\nPipeline Concluído com Sucesso. Obrigado!\n")

# Pipeline de Analytics 1 - Busca o artista e o comprimento (tempo) da música do sessionId = 436 e itemInSession = 12
def pipeline_analytics_1(session):

    print("\nIniciando o Pipeline de Analytics 1 (Carga e Análise de Dados)...")

    # Cria a tabela
    query = "CREATE TABLE IF NOT EXISTS tb_session_itemSession "
    
    # Cria as colunas na tabela
    query = query + "(sessionId text, itemInSession text, song text, artist text, length text, " \
                    "PRIMARY KEY (sessionId, itemInSession))"
    
    # Executa
    session.execute(query)

    # Abre o arquivo de entrada para a carga de dados
    with open(file_name, 'r', encoding = 'utf-8') as fh:

        # Leitura do arquivo
        reader = csv.reader(fh)
        next(reader) #esse next pula a primeira linha, o cabeçalho

        # Loop pelo arquivo e carga na tebal no Cassandra
        for line in reader:
            query = "INSERT INTO tb_session_itemSession (sessionId, itemInSession, song, artist, length)"
            query = query + " VALUES (%s, %s, %s, %s, %s)"
            session.execute(query, (line[8], line[3], line[9], line[0], line[5]))

    # Select nos dados
    query = "SELECT artist, song, length FROM tb_session_itemSession WHERE sessionId = '436' and itemInSession = '12'"
    
    # Converte o resultado em dataframe do Pandas e salva em disco
    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline1.csv', sep = ',', encoding = 'utf-8')
    
    # Print
    print("\nResultado do Pipeline de Analytics 1:\n")
    print(df)

# Pipeline de Analytics 2 - Busca o artista, o nome da música e o usuário do userid = 54 e sessionid = 616
def pipeline_analytics_2(session):

    print("\nIniciando o Pipeline de Analytics 2 (Carga e Análise de Dados)...")

    # Cria a tabela
    query = "CREATE TABLE IF NOT EXISTS tb_user_session "
    query = query + "(userId text, sessionId text, itemInSession text, artist text, song text, firstName text, " \
                    "lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"
    session.execute(query)

    # Carrega a tabela
    with open(file_name, 'r', encoding = 'utf8') as f:
        reader = csv.reader(f)
        next(reader) 
        for line in reader:
            query = "INSERT INTO tb_user_session (userId, sessionId, itemInSession, artist, song, firstName, lastName)"
            query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (line[10], line[8], line[3], line[0], line[9], line[1], line[4]))

    # Analisa a tabela
    query = "SELECT artist, song, firstname, lastname FROM tb_user_session WHERE userId = '54' and sessionId = '616'"

    # Resultado
    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline2.csv', sep = ',', encoding = 'utf-8')
    print("\nResultado do Pipeline de Analytics 2:\n")
    print(df)

# Pipeline de Analytics 3 - Busca cada usuário que ouviu a música 'The Rhythm Of The Night'
def pipeline_analytics_3(session):

    print("\nIniciando o Pipeline de Analytics 3 (Carga e Análise de Dados)...")

    # Cria a tabela
    query = "CREATE TABLE IF NOT EXISTS tb_user_song "
    query = query + "(song text, userId text, firstName text, lastName text, PRIMARY KEY (song, userId))"
    session.execute(query)

    # Carrega a tabela
    with open(file_name, 'r', encoding = 'utf8') as f:
        reader = csv.reader(f)
        next(reader) 
        for line in reader:
            query = "INSERT INTO tb_user_song (song, userId, firstName, lastName)"
            query = query + " VALUES (%s, %s, %s, %s)"
            session.execute(query, (line[9], line[10], line[1], line[4]))

    # Analisa a tabela
    query = "SELECT firstname, lastname FROM tb_user_song WHERE song = 'The Rhythm Of The Night'"
    
    # Resultado
    df = pd.DataFrame(list(session.execute(query)))
    df.to_csv('resultado/pipeline3.csv', sep = ',', encoding = 'utf-8')
    print("\nResultado do Pipeline de Analytics 3:\n")
    print(df)

# Função para deletar as tabelas ao final do pipeline
def drop_tables(session):

    query = "DROP TABLE tb_session_itemSession"
    session.execute(query)

    query = "DROP TABLE tb_user_session"
    session.execute(query)

    query = "DROP TABLE tb_user_song"
    session.execute(query)




