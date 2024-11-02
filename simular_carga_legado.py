from config import get_connection
from datetime import datetime
import uuid
import random

# Função para gerar dados de cliente
def gerar_dados_cliente():
    return {
        "cliente_id": str(uuid.uuid4()),  # Gera um ID único
        "nome": f"Cliente_{random.randint(1000, 9999)}",
        "endereco": f"Rua {random.randint(1, 100)}, Cidade {random.randint(1, 10)}",
        "load_date": datetime.now()
    }

# Função para gerar dados de pedido
def gerar_dados_pedido():
    return {
        "pedido_id": str(uuid.uuid4()),  # Gera um ID único
        "valor": round(random.uniform(50, 500), 2),  # Valor aleatório entre 50 e 500
        "status": random.choice(["Novo", "Processando", "Concluído"]),
        "load_date": datetime.now()
    }

# Funções para inserir dados no Hub, Link e Satélites
def inserir_multiplos_hub_cliente(clientes):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for cliente in clientes:
                cursor.execute('''
                    INSERT INTO Hub_Cliente (cliente_id, load_date) 
                    VALUES (%s, %s)
                ''', (cliente["cliente_id"], cliente["load_date"]))
            conn.commit()
            print("Clientes inseridos no Hub_Cliente.")

def inserir_multiplos_hub_pedido(pedidos):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for pedido in pedidos:
                cursor.execute('''
                    INSERT INTO Hub_Pedido (pedido_id, load_date) 
                    VALUES (%s, %s)
                ''', (pedido["pedido_id"], pedido["load_date"]))
            conn.commit()
            print("Pedidos inseridos no Hub_Pedido.")

def inserir_multiplos_links(clientes, pedidos):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for cliente, pedido in zip(clientes, pedidos):
                link_id = str(uuid.uuid4())
                cursor.execute('''
                    INSERT INTO Link_Cliente_Pedido (link_id, cliente_id, pedido_id, load_date) 
                    VALUES (%s, %s, %s, %s)
                ''', (link_id, cliente["cliente_id"], pedido["pedido_id"], datetime.now()))
            conn.commit()
            print("Links inseridos no Link_Cliente_Pedido.")

def inserir_multiplos_sat_cliente(clientes):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for cliente in clientes:
                cursor.execute('''
                    INSERT INTO Sat_Cliente (cliente_id, nome, endereco, vigencia_inicio, vigencia_fim, load_date)
                    VALUES (%s, %s, %s, %s, NULL, %s)
                ''', (cliente["cliente_id"], cliente["nome"], cliente["endereco"], cliente["load_date"], datetime.now()))
            conn.commit()
            print("Clientes inseridos no Sat_Cliente.")

def inserir_multiplos_sat_pedido(pedidos):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for pedido in pedidos:
                cursor.execute('''
                    INSERT INTO Sat_Pedido (pedido_id, valor, status, vigencia_inicio, vigencia_fim, load_date)
                    VALUES (%s, %s, %s, %s, NULL, %s)
                ''', (pedido["pedido_id"], pedido["valor"], pedido["status"], pedido["load_date"], datetime.now()))
            conn.commit()
            print("Pedidos inseridos no Sat_Pedido.")

# Função principal para simulação
def simular_carga_legado():
    # Gerar dados de clientes e pedidos simulados
    clientes = [gerar_dados_cliente() for _ in range(5)]
    pedidos = [gerar_dados_pedido() for _ in range(5)]

    # Inserir os dados nas tabelas do Data Vault
    inserir_multiplos_hub_cliente(clientes)
    inserir_multiplos_hub_pedido(pedidos)
    inserir_multiplos_links(clientes, pedidos)
    inserir_multiplos_sat_cliente(clientes)
    inserir_multiplos_sat_pedido(pedidos)
    print("Simulação de carga de sistema legado concluída.")

if __name__ == "__main__":
    simular_carga_legado()
