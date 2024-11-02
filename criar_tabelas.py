# criar_tabelas.py
from config import get_connection

def criar_tabelas():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            # Criar Hub de Clientes
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Hub_Cliente (
                    cliente_id TEXT PRIMARY KEY,
                    load_date TIMESTAMP
                )
            ''')

            # Criar Hub de Pedidos
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Hub_Pedido (
                    pedido_id TEXT PRIMARY KEY,
                    load_date TIMESTAMP
                )
            ''')

            # Criar Link Cliente-Pedido
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Link_Cliente_Pedido (
                    link_id TEXT PRIMARY KEY,
                    cliente_id TEXT REFERENCES Hub_Cliente(cliente_id),
                    pedido_id TEXT REFERENCES Hub_Pedido(pedido_id),
                    load_date TIMESTAMP
                )
            ''')

            # Criar Satélite de Cliente
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Sat_Cliente (
                    cliente_id TEXT REFERENCES Hub_Cliente(cliente_id),
                    nome TEXT,
                    endereco TEXT,
                    vigencia_inicio TIMESTAMP,
                    vigencia_fim TIMESTAMP NULL,
                    load_date TIMESTAMP
                )
            ''')

            # Criar Satélite de Pedido
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Sat_Pedido (
                    pedido_id TEXT REFERENCES Hub_Pedido(pedido_id),
                    valor REAL,
                    status TEXT,
                    vigencia_inicio TIMESTAMP,
                    vigencia_fim TIMESTAMP NULL,
                    load_date TIMESTAMP
                )
            ''')
            conn.commit()
            print("Tabelas criadas com sucesso.")

if __name__ == "__main__":
    criar_tabelas()
