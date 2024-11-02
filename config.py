# config.py
import psycopg2

# Configuração da conexão com o PostgreSQL
def get_connection():
    return psycopg2.connect(
        host="",
        database="database_data_vault",
        user="database_data_vault_user",
        password=""
    )
