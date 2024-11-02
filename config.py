# config.py
import psycopg2

# Configuração da conexão com o PostgreSQL
def get_connection():
    return psycopg2.connect(
        host="csj85ra3esus7381vvd0-a.oregon-postgres.render.com",
        database="database_data_vault",
        user="database_data_vault_user",
        password="KoInF5F0MC3dpUZ8F1DhNKuOTLhYBFYy"
    )
