import requests
import os
import json
import psycopg2
from datetime import date
from dotenv import load_dotenv

from prefect import flow, task

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Variáveis de configuração
client_id = os.getenv("RDSTATION_CLIENT_ID")
client_secret = os.getenv("RDSTATION_CLIENT_SECRET")
token_url = 'https://api.rd.services/auth/token'
api_url = 'https://api.rd.services/platform/contacts/fields' 

refresh_token = os.getenv("RDSTATION_REFRESH_TOKEN")
access_token = os.getenv("RDSTATION_ACCESS_TOKEN")

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")

@task
def verificar_validade_token(access_token):
    """
    Verifica se o token de acesso é válido.
    """
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        print('Token válido.')
    elif response.status_code == 401:
        print('Token expirado. Renovando...')
        renovar_token()
    else:
        print(f'Erro na verificação do token. Status code: {response.status_code}')

@task
def renovar_token():
    """
    Renova o token de acesso usando o refresh token.
    """
    global refresh_token
    global access_token

    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }

    response = requests.post(token_url, data=data)

    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data['access_token']
        refresh_token = token_data.get('refresh_token', refresh_token)  # Atualiza se houver novo refresh token
        print('Token renovado com sucesso.')
    else:
        print(f'Erro na renovação do token. Status code: {response.status_code}')

@task
def criar_json_data():
    """
    Cria um dicionário JSON com os dados do token e credenciais.
    """
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "access_token": access_token,
        "access_token_refreshdate": date.today()
    }
    return data

@task
def atualizar_banco_dados(dados):
    """
    Atualiza o banco de dados com os dados do token e credenciais.
    """
    db_config = {
        "dbname": "rd_station",
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": 25060
    }

    try:
        # Conexão ao banco de dados
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Query de inserção
        update_query = """
            UPDATE token.access_credentials
            SET access_token = %s, access_token_refreshdate = %s
            WHERE client_secret = %s
        """

        # Executar a query com os dados
        cursor.execute(
            update_query,
            (
                dados['access_token'], 
                dados['access_token_refreshdate'],
                dados['client_secret']
            )
)

        # Confirmar as alterações
        conn.commit()
        print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        # Fechar conexões com o banco de dados
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@flow(log_prints=True)
def validation_flow():

    # Verifica se o token é válido
    verificar_validade_token(access_token)

    # Gera os dados em formato JSON
    dados_para_upload = criar_json_data()

    # Exibe os dados gerados
    print(json.dumps(dados_para_upload, indent=4, default=str))

    # Atualiza o banco de dados
    atualizar_banco_dados(dados_para_upload)


if __name__ == "__main__":
    validation_flow()