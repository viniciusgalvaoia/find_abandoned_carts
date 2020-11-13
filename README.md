# Carrinhos abandonados pelos clientes de um e-commerce

## Introdução

Um dos problemas mais clássicos do mercado de e-commerce é o carrinho abandonado.

Analisar os dados desse evento é fundamental para que as empresas tentem entender o porquê que os clientes estão desistindo das compras.

Esse projeto visa ajudar uma empresa de e-commerce a coletar informações relacionadas a esse evento.


## Dados e ETL Pipeline

### Dados

Foi utilizado os conjuntos de dados, page-views, referente a navegação dos clientes nas páginas do site.

### ETL Pipeline

O ETL pipeline extrai os dados do arquivo `page-views.json`, cria um arquivo temporário chamado `page-views_wrk.json`, contendo novas chaves e valores que irão auxiliar o resto do pipeline a encontrar as situações de carrinho abandonado utilizando o script `find_abandoned_carts.py` e salvando os resultados no arquivo `abandoned-carts.json`. 

## Como executar

### Pré-requisitos

- Python 3
- apache-beam==2.25.0
- argparse==1.1
- logging==0.5.1.2
- json==2.0.9
- pandas==1.0.5

### Instruções

Executar código: `python find_abandoned_carts.py --input input/page-views.json --output output/abandoned-carts.json`

## Arquivos do projeto

- **input**
  - **page-views.json**
    - dados de navegação dos clientes nas páginas do vendas
  - **page-views_wrk.json**
    - arquivo temporário
- **output**
  - **abandoned-carts.json**
    - dados dos carrinhos abandonados
- **find_abandoned_carts.py**
  - Script para implementaçao do processo de ETL
  - Quando executado, esse script irá
    - Extrair os dados do arquivo page-views.json
    - Criar um novo arquivo temporário page-views_wrk.json
    - Filtrar os casos em que ocorre abandono de carrinho
    - Criar arquivo abandoned-carts.json com os dados de carrinhos abandonados
- **README.md**
  - Descrição e instruções sobre o projeto
