# Script ETL para Extração e Transformação (Carga e Analytics está no outro script)

# Imports
import os
import glob
import csv
from pipeline import conecta_cluster



# EXTRAÇÃO!

# Função para consolidar(processar) os arquivos de entrada em um único arquivo
# Processo os 30 arquivos colocando todos os dados dos arquivos em uma lista (na memoria do computador)
def etl_processa_arquivos():

    print("\nIniciando a Etapa 1 do ETL...")

    # Pasta com os arquivos que serão processados
    current_path = "dados"
    print("\nOs arquivos que serão processados estão na pasta: " + current_path)

    # Lista para o caminho de cada arquivo
    lista_caminho_arquivos = []

    # Loop para extrair o caminho de cada arquivo (poderia extrair de um banco de dados, de um data lake, etc...)
    print("\nExtraindo o caminho de cada arquivo.")
    for root, dirs, files in os.walk(current_path):
        lista_caminho_arquivos = glob.glob(os.path.join(root, '*'))

    # Lista para manipular as linhas de cada arquivo
    linhas_dados_all = list()

    # Loop por cada arquivo
    print("\nExtraindo as linhas de cada arquivo e consolidando em um único arquivo.")
    for file in lista_caminho_arquivos:
        with open(file, 'r', encoding = 'utf8', newline = '') as fh:
            reader = csv.reader(fh)
            next(reader)

            # Append de cada linha de cada arquivo
            for line in reader:
                linhas_dados_all.append(line)

    print("\nEtapa 1 Finalizada. Extração concluída com sucesso.")

    return linhas_dados_all




# TRANSFORMAÇÃO!!

# Função para extrair somente os dados relevantes do arquivo gerado pela função anterior 
def etl_processa_dados(records):

    print("\nIniciando a Etapa 2 do ETL...")

    # Registra uma estrutura de dados
    csv.register_dialect('dadosGerais', quoting = csv.QUOTE_ALL, skipinitialspace = True)

    # Filtrando os dados relevantes que serão inseridos no Apache Cassandra
    print("\nFiltrando os dados relevantes que serão inseridos no Apache Cassandra.")
    with open('resultado/dataset_completo.csv', 'w', encoding = 'utf8', newline = '') as fh:

        # Cria o objeto
        writer = csv.writer(fh)

        # Executa
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length','level', 'location', 'sessionId', 'song', 'userId'])

        # Loop
        for row in records:
            # Se não tiver artista, não geramos a linha final
            if not row[0]:
                continue
            
            # Gravo a linha de dados
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

        print("\nEtapa 2 Finalizada. Transformação concluída com sucesso.")




# Bloco main
if __name__ == '__main__':
    
    # Etapa 1 do ETL
    records_list = etl_processa_arquivos()

    # Se a Etapa 1 foi executada com sucesso, seguimos para a próxima etapa
    if records_list:

        # Etapa 2 do ETL 
        etl_processa_dados(records_list)

        # Conectamos no cluster para a carga de dados no Cassandra
        conecta_cluster()





