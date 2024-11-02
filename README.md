
O Data Vault é uma abordagem de modelagem de dados que organiza os dados em três entidades principais: Hubs, Links e Satélites. Esse modelo é usado principalmente em data warehouses, porque facilita a ingestão de dados de diferentes fontes e permite uma modelagem histórica e rastreável. Vou explicar cada um desses elementos e, em seguida, daremos um exemplo prático em Python e SQL para entender como funciona.


1. Conceitos do Data Vault

Hub: Armazena chaves únicas de negócios (como ID de cliente ou ID de pedido) e funciona como uma "chave central" para a entidade. Cada Hub representa uma entidade central no modelo (ex., clientes, produtos, pedidos).

Link: Conecta Hubs entre si, modelando os relacionamentos entre as entidades (por exemplo, o relacionamento entre cliente e pedido). Cada Link mantém as associações com base em chaves de negócios.

Satélite: Armazena atributos e detalhes de uma entidade específica (por exemplo, nome e endereço do cliente), além de um registro histórico. Cada vez que um atributo muda, um novo registro é adicionado ao Satélite com a data de vigência, preservando o histórico.

### 1. Configuração Inicial
Antes de tudo, vamos garantir que o PostgreSQL esteja acessível e que o Python esteja configurado para se conectar ao banco de dados.

Certifique-se de ter o PostgreSQL em execução e um banco de dados criado (ex., `datavault_db`). Vamos instalar a biblioteca `psycopg2` para permitir a conexão entre o Python e o PostgreSQL:

```bash
pip install psycopg2
```

### Estrutura do Código

- **Configuração da Conexão com o PostgreSQL**
- **Rotina para Criar as Tabelas** (Hubs, Links e Satélites)
- **Rotina para Inserir Dados nos Hubs**
- **Rotina para Inserir Dados nos Links**
- **Rotina para Inserir Dados nos Satélites**

Para organizar melhor as funções em scripts individuais, cada uma com um propósito específico, e facilitar a execução no Visual Studio Code, vamos dividir o código em quatro arquivos Python:

config.py: Contém a configuração da conexão.
criar_tabelas.py: Cria todas as tabelas do modelo Data Vault.
inserir_hubs_links.py: Insere dados nos Hubs e Links.
inserir_satelites.py: Insere dados nos Satélites.
Aqui está o conteúdo de cada arquivo, incluindo exemplos de chamadas.

1. Arquivo config.py
Este arquivo contém a função para configurar a conexão com o PostgreSQL.

python
Copiar código
# config.py
import psycopg2

# Configuração da conexão com o PostgreSQL
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="datavault_db",
        user="seu_usuario",
        password="sua_senha"
    )
2. Arquivo criar_tabelas.py
Este arquivo cria as tabelas do modelo Data Vault (Hubs, Links, e Satélites).

python
Copiar código
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
Para rodar, execute no terminal:

bash
Copiar código
python criar_tabelas.py
3. Arquivo inserir_hubs_links.py
Este arquivo contém as funções para inserir dados nos Hubs e Links.

python
Copiar código
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
Para rodar, execute no terminal:

bash
Copiar código
python inserir_hubs_links.py
4. Arquivo inserir_satelites.py
Este arquivo contém as funções para inserir dados nos Satélites.

python
Copiar código
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
Para rodar, execute no terminal:

bash
Copiar código
python inserir_satelites.py
Esses arquivos permitem que você execute as tarefas em etapas e facilita o entendimento e manutenção das rotinas.


Arquivo inserir_multiplos_registros.py
Este script insere cinco novos registros nos Hubs de Cliente e Pedido, cria Links entre os Clientes e Pedidos, e adiciona informações nos Satélites de Cliente e Pedido.

python
Copiar código
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
Explicação das Funções
inserir_multiplos_hub_cliente: Insere múltiplos clientes no Hub de Cliente.
inserir_multiplos_hub_pedido: Insere múltiplos pedidos no Hub de Pedido.
inserir_multiplos_links: Cria links entre clientes e pedidos.
inserir_multiplos_sat_cliente: Insere detalhes dos clientes no Satélite de Cliente.
inserir_multiplos_sat_pedido: Insere detalhes dos pedidos no Satélite de Pedido.
Execução
Para rodar este script e inserir múltiplos registros, execute no terminal:

bash
Copiar código
python inserir_multiplos_registros.py
Resultado Esperado
Ao executar o script, ele deve exibir mensagens confirmando que os registros foram inseridos em cada tabela, como:

plaintext
Copiar código
5 registros inseridos no Hub de Clientes.
5 registros inseridos no Hub de Pedidos.
5 links inseridos entre Clientes e Pedidos.
5 registros inseridos no Satélite de Clientes.
5 registros inseridos no Satélite de Pedidos.
Isso vai popular as tabelas Hub_Cliente, Hub_Pedido, Link_Cliente_Pedido, Sat_Cliente e Sat_Pedido com dados de exemplo.

