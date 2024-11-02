# inserir_multiplos_registros.py
from config import get_connection
from datetime import datetime
import uuid

# Função para inserir múltiplos clientes no Hub_Cliente
def inserir_multiplos_hub_cliente(clientes):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for cliente_id in clientes:
                cursor.execute('''
                    INSERT INTO Hub_Cliente (cliente_id, load_date)
                    VALUES (%s, %s)
                ''', (cliente_id, datetime.now()))
            conn.commit()
            print(f"{len(clientes)} registros inseridos no Hub de Clientes.")

# Função para inserir múltiplos pedidos no Hub_Pedido
def inserir_multiplos_hub_pedido(pedidos):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for pedido_id in pedidos:
                cursor.execute('''
                    INSERT INTO Hub_Pedido (pedido_id, load_date)
                    VALUES (%s, %s)
                ''', (pedido_id, datetime.now()))
            conn.commit()
            print(f"{len(pedidos)} registros inseridos no Hub de Pedidos.")

# Função para inserir múltiplos links Cliente-Pedido
def inserir_multiplos_links(clientes, pedidos):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for cliente_id, pedido_id in zip(clientes, pedidos):
                link_id = str(uuid.uuid4())  # Gerar um ID único para cada link
                cursor.execute('''
                    INSERT INTO Link_Cliente_Pedido (link_id, cliente_id, pedido_id, load_date)
                    VALUES (%s, %s, %s, %s)
                ''', (link_id, cliente_id, pedido_id, datetime.now()))
            conn.commit()
            print(f"{len(clientes)} links inseridos entre Clientes e Pedidos.")

# Função para inserir múltiplos registros no Satélite de Clientes
def inserir_multiplos_sat_cliente(clientes_dados):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for cliente_id, nome, endereco in clientes_dados:
                cursor.execute('''
                    INSERT INTO Sat_Cliente (cliente_id, nome, endereco, vigencia_inicio, vigencia_fim, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (cliente_id, nome, endereco, datetime.now(), None, datetime.now()))
            conn.commit()
            print(f"{len(clientes_dados)} registros inseridos no Satélite de Clientes.")

# Função para inserir múltiplos registros no Satélite de Pedidos
def inserir_multiplos_sat_pedido(pedidos_dados):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for pedido_id, valor, status in pedidos_dados:
                cursor.execute('''
                    INSERT INTO Sat_Pedido (pedido_id, valor, status, vigencia_inicio, vigencia_fim, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (pedido_id, valor, status, datetime.now(), None, datetime.now()))
            conn.commit()
            print(f"{len(pedidos_dados)} registros inseridos no Satélite de Pedidos.")

if __name__ == "__main__":
    # Dados fictícios para carga inicial
    clientes = [f'C00{i}' for i in range(2, 7)]  # C002, C003, ..., C006
    pedidos = [f'P00{i}' for i in range(2, 7)]   # P002, P003, ..., P006

    # Dados detalhados para o Satélite de Cliente (cliente_id, nome, endereco)
    clientes_dados = [
        ('C002', 'Ana Costa', 'Rua B, 234'),
        ('C003', 'Carlos Nunes', 'Av. Central, 456'),
        ('C004', 'Maria Oliveira', 'Rua C, 789'),
        ('C005', 'Pedro Martins', 'Praça D, 101'),
        ('C006', 'Joana Silva', 'Av. Norte, 202')
    ]

    # Dados detalhados para o Satélite de Pedido (pedido_id, valor, status)
    pedidos_dados = [
        ('P002', 300.00, 'Concluído'),
        ('P003', 150.50, 'Cancelado'),
        ('P004', 200.00, 'Em Processamento'),
        ('P005', 450.75, 'Concluído'),
        ('P006', 120.00, 'Em Processamento')
    ]

    # Executa as funções de inserção
    inserir_multiplos_hub_cliente(clientes)
    inserir_multiplos_hub_pedido(pedidos)
    inserir_multiplos_links(clientes, pedidos)
    inserir_multiplos_sat_cliente(clientes_dados)
    inserir_multiplos_sat_pedido(pedidos_dados)
