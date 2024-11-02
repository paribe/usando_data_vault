# inserir_satelites.py
from config import get_connection
from datetime import datetime

def inserir_sat_cliente(cliente_id, nome, endereco):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO Sat_Cliente (cliente_id, nome, endereco, vigencia_inicio, vigencia_fim, load_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (cliente_id, nome, endereco, datetime.now(), None, datetime.now()))
            conn.commit()
            print(f"Dados do cliente {cliente_id} inseridos no Satélite de Cliente.")

def inserir_sat_pedido(pedido_id, valor, status):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO Sat_Pedido (pedido_id, valor, status, vigencia_inicio, vigencia_fim, load_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (pedido_id, valor, status, datetime.now(), None, datetime.now()))
            conn.commit()
            print(f"Dados do pedido {pedido_id} inseridos no Satélite de Pedido.")

if __name__ == "__main__":
    # Exemplo de uso
    inserir_sat_cliente('C001', 'João Silva', 'Rua A, 123')
    inserir_sat_pedido('P001', 250.00, 'Em Processamento')
