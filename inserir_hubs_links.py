# inserir_hubs_links.py
from config import get_connection
from datetime import datetime

def inserir_hub_cliente(cliente_id):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO Hub_Cliente (cliente_id, load_date)
                VALUES (%s, %s)
            ''', (cliente_id, datetime.now()))
            conn.commit()
            print(f"Cliente {cliente_id} inserido no Hub de Clientes.")

def inserir_hub_pedido(pedido_id):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO Hub_Pedido (pedido_id, load_date)
                VALUES (%s, %s)
            ''', (pedido_id, datetime.now()))
            conn.commit()
            print(f"Pedido {pedido_id} inserido no Hub de Pedidos.")

def inserir_link_cliente_pedido(link_id, cliente_id, pedido_id):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO Link_Cliente_Pedido (link_id, cliente_id, pedido_id, load_date)
                VALUES (%s, %s, %s, %s)
            ''', (link_id, cliente_id, pedido_id, datetime.now()))
            conn.commit()
            print(f"Link {link_id} entre Cliente {cliente_id} e Pedido {pedido_id} inserido.")

if __name__ == "__main__":
    # Exemplo de uso
    inserir_hub_cliente('C001')
    inserir_hub_pedido('P001')
    inserir_link_cliente_pedido('L001', 'C001', 'P001')
