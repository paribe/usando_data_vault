
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


# Arquivo inserir_multiplos_registros.py
Este script insere cinco novos registros nos Hubs de Cliente e Pedido, cria Links entre os Clientes e Pedidos, e adiciona informações nos Satélites de Cliente e Pedido.

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


# Explicação do Código
Gerar Dados Fictícios:

gerar_dados_cliente() cria dados de cliente com um ID único e outras informações fictícias.
gerar_dados_pedido() cria dados de pedido, incluindo um ID, valor e status aleatórios.
Inserir Dados no Data Vault:

Hubs: Funções inserir_multiplos_hub_cliente e inserir_multiplos_hub_pedido inserem clientes e pedidos no hub correspondente.
Links: inserir_multiplos_links cria relacionamentos entre cliente e pedido.
Satélites: inserir_multiplos_sat_cliente e inserir_multiplos_sat_pedido inserem detalhes adicionais de cliente e pedido nos satélites.
Simulação: simular_carga_legado() gera cinco registros de cliente e cinco de pedido, inserindo-os na estrutura de Data Vault.

Execução
Ao rodar simular_carga_legado(), a função insere novos dados simulados no Data Vault, imitando uma carga de dados vinda de um sistema legado. Essa simulação permite observar como o Data Vault se comporta ao receber dados novos de clientes e pedidos.