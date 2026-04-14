# Case Técnico: Pipeline de Dados Analítico
### Solução de Engenharia de Dados com Arquitetura de Medalhão

**Autor:** Nathan B. Cunha  
**Data:** 14 de Abril de 2026  
**Versão:** 1.0  

---

## 1. Introdução
Esta documentação detalha a implementação técnica do pipeline de dados desenvolvido para o desafio de Engenharia de Dados. O objetivo principal é a transformação de fontes de dados heterogêneas (**Vendas, Logística, Produtos, Clientes e Vendedores**) em uma base analítica fiável e de alto desempenho, orientada para o consumo por ferramentas de Business Intelligence (BI).

## 2. Arquitetura da Solução
A solução foi construída no ambiente **Databricks Community Edition**, utilizando a **Arquitetura de Medalhão (Medallion Architecture)** para garantir a rastreabilidade, qualidade e governança dos dados.

### 2.1. Camadas de Dados
* **Raw (Bronze):** Armazenamento dos dados brutos em formato Delta, preservando a fidelidade das fontes originais.
* **Refined (Silver):** Camada de limpeza e normalização. Aplicação de tipagens (`Casts`), tratamento de valores nulos e regras de integridade referencial.
* **Gold (Ouro):** Camada de modelagem dimensional (**Star Schema**). Contém as tabelas fato e dimensão otimizadas para consulta e a View Semântica de apresentação.

---

## 3. Qualidade de Dados e Tratamentos (Data Quality)
A fase de tratamento foi crítica para garantir que as métricas de negócio não fossem distorcidas por inconsistências das fontes.

### 3.1. Deduplicação Estratégica
Identificou-se uma inconsistência na fonte de Logística, onde o pedido `O00185` apresentava duplicidade. Foi aplicada uma lógica de deduplicação baseada no `order_id` para garantir que o *Join* com a tabela de Vendas mantivesse a volumetria correta de **995 registros**.

### 3.2. Normalização de Valores Financeiros
* **Conversão de Tipos:** Valores de preço (String com vírgula) foram convertidos para `Decimal`.
* **Proteção de KPIs:** O cálculo do Ticket Médio foi formatado utilizando `CAST(... AS DECIMAL(18,2))` para evitar notação científica no dashboard final.

### 3.3. Integridade Referencial
Filtros aplicados para remover **"itens órfãos"** — registros de vendas sem correspondência válida nas tabelas de suporte, garantindo a consistência dos *Joins* na camada Gold.

---

## 4. Modelagem Dimensional (Camada Gold)
Estrutura baseada em **Star Schema** para facilitar o consumo *self-service* de BI.

### 4.1. Tabelas de Dimensão
* `dim_calendar`: Gerada dinamicamente (2024-2026) para evolução temporal (Ano, Mês, Trimestre).
* `dim_products_gold`: Cadastro limpo de produtos e categorias.
* `dim_sellers`: Informações de vendedores e canais de venda.
* `dim_locations_gold`: Hierarquia geográfica (Região e Estado).

### 4.2. Tabela Fato
* `fct_sales_master`: Consolidação central de vendas e logística, com métricas de quantidade, receita líquida e flags de status.

---

## 5. Camada de Apresentação (View Semântica)
Criada a view `vw_analytics_master` (One Big Table), contendo:
* **Atributos de Tempo:** Ano, Mês e Trimestre.
* **Atributos de Produto:** Nome, Categoria e Subcategoria.
* **Atributos Regionais:** Canal, Região e Estado (`uf`).
* **Indicadores (KPIs):** Receita Líquida, Quantidade, Valor de Frete, Flag de Cancelamento e Flag de Atraso (`is_delayed`).
* **Métricas Calculadas:** Ticket Médio por item.

---

## 6. Instruções de Execução
Os notebooks devem ser executados na seguinte ordem lógica:

1.  `00_setup_config`: Definição de variáveis e caminhos de catálogo.
2.  `01_ingestion_raw`: Carga dos arquivos para a camada Bronze.
3.  `02_refined_layer`: Limpeza e deduplicação (Camada Silver).
4.  `03_gold_layer`: Criação do modelo físico Star Schema.
5.  `04_data_quality_checks`: Validação de volumetria e integridade.
6.  `05_dataviz`: Criação da View de Analytics final.

---

## 7. Próximos Passos e Evolução

* **Automação e CI/CD:** Implementação de *Databricks Workflows* para orquestração e pipelines de CI/CD para integração segura de código.
* **Qualidade e Contratos:** Inclusão de *Data Contracts* e testes avançados (**Great Expectations**) como *gates* entre todas as camadas.
* **Processamento Incremental:** Transição para carga incremental via *Auto Loader*, reduzindo custos e latência.
* **Governança:** Utilização do *Unity Catalog* para controle de acesso e linhagem de dados (*Data Lineage*).
* **Monitoramento:** Alertas em tempo real (Slack/Teams) para falhas ou anomalias de volumetria.
* **Facilidades para Analytics:**
    * **Catálogo:** Dicionário de dados para descoberta de métricas.
    * **Performance:** Aplicação de *Z-Ordering* em colunas de filtro frequentes.
    * **Self-Service:** Criação de visões específicas por unidade de negócio.

---

## Conclusão
Este projeto validou a viabilidade técnica da arquitetura de medalhão no Databricks. Em um cenário real, a ingestão manual aqui realizada seria substituída por fontes automáticas (Streaming/Batch) e protegida por sistemas de alerta e controle de qualidade rigorosos. A estrutura entregue provê uma base sólida, performática e confiável para a geração de insights estratégicos.
