# 🏗️ Tech Challenge 03 - Data Warehouse Coffee Sales com LLM

Um projeto completo de análise de dados de vendas de café utilizando Data Warehouse, API REST e técnicas de LLM (Large Language Models) para insights inteligentes.

## 📁 Estrutura do Projeto

```
techchallenge_03/
├── 📋 README.md                        # Documentação do projeto
├── 📋 SIMPLIFICACAO_DOCKER_COMPOSE.md  # Documentação adicional
├── 🐳 docker-compose.yml               # Orquestração de containers
├── 🐳 Containerfile.api                # Containerfile para API
├── 🐳 Containerfile.postgres           # Containerfile para PostgreSQL
├── 🚀 start-api.sh                     # Script de inicialização
└── 📁 src/                             # Código fonte
    ├── 📊 analise_dw_coffee_llm.ipynb  # Notebook principal de análise
    ├── 🚀 api.py                       # API REST FastAPI
    ├── 📥 data_upload.py               # Download de datasets
    ├── 💾 persistencia.py              # Persistência no PostgreSQL
    ├── 🏗️ dw_tratamento.py             # Tratamento e criação do DW
    ├── 📋 requirements.txt             # Dependências Python
```

## 🛠️ Tecnologias e Bibliotecas Utilizadas

### Backend e API
- **FastAPI** 🚀 - Framework web moderno para APIs
- **Uvicorn** ⚡ - Servidor ASGI de alta performance
- **Pydantic** 📋 - Validação de dados

### Banco de Dados
- **PostgreSQL 15** 🐘 - Banco de dados relacional
- **SQLAlchemy** 🔧 - ORM Python
- **psycopg2-binary** 🔌 - Driver PostgreSQL

### Análise de Dados
- **Pandas** 🐼 - Manipulação e análise de dados
- **NumPy** 🔢 - Computação numérica
- **Matplotlib** 📊 - Visualizações básicas
- **Seaborn** 🎨 - Visualizações estatísticas
- **Plotly** 📈 - Gráficos interativos

### Ambiente de Desenvolvimento
- **Jupyter Notebook** 📓 - Ambiente interativo
- **Python 3.11** 🐍 - Linguagem de programação
- **Podman** 🐳 - Containerização

### Outras Dependências
- **KaggleHub** 📦 - Download de datasets
- **python-dotenv** ⚙️ - Variáveis de ambiente
- **Kaleido** 🖼️ - Exportação de gráficos Plotly

## 🔧 Requisitos para Execução

### Pré-requisitos
- **Podman** instalado
- **Podman Compose** instalado
- **8GB RAM** recomendado
- **2GB** de espaço em disco

### Verificar Instalação
```bash
# Verificar Podman
podman --version

# Verificar Podman Compose
podman-compose --version
```

## 🚀 Como Executar a Aplicação

### 1. Clone o Repositório
```bash
git clone https://github.com/Machine-Learning-Engineering/techchallenge_03.git
cd techchallenge_03
```

### 2. Iniciar a Aplicação Completa
```bash
# Com Podman
podman-compose up --build --no-cache
```

### 3. Aguardar Inicialização
O sistema inicializará na seguinte ordem:
1. **PostgreSQL** (porta 5432) 🐘
2. **Inicialização do banco** (criação de estruturas)
3. **API FastAPI** (porta 8000) 🚀
4. **Jupyter Notebook** (porta 8888) 📓

### 4. Verificar Serviços
```bash
# Verificar containers em execução
podman ps
```

## 🌐 Como Executar as APIs

### Documentação Swagger
Acesse a documentação interativa da API:
- **URL**: http://localhost:8000/docs
- **Alternativa ReDoc**: http://localhost:8000/redoc

### Endpoints Principais

#### 1. Pipeline Completo (Recomendado)
```bash
# Executar todo o pipeline automaticamente
curl -X POST "http://localhost:8000/pipeline/execute" \
     -H "Content-Type: application/json"
```

#### 2. Execução Passo a Passo

**Download dos Dados:**
```bash
curl -X POST "http://localhost:8000/data-upload/" \
     -H "Content-Type: application/json" \
     -d '{"force_download": false}'
```

**Persistência no PostgreSQL:**
```bash
curl -X POST "http://localhost:8000/persistencia/" \
     -H "Content-Type: application/json" \
     -d '{"force_reload": false}'
```

**Criação do Data Warehouse:**
```bash
curl -X POST "http://localhost:8000/dw-tratamento/" \
     -H "Content-Type: application/json" \
     -d '{"force_rebuild": false}'
```

#### 3. Verificação de Status
```bash
# Status da API
curl -X GET "http://localhost:8000/"

# Saúde da aplicação
curl -X GET "http://localhost:8000/health"
```

## 📓 Como Executar o Notebook

### 1. Acessar Jupyter Notebook
Abra seu navegador em: **http://localhost:8888**

### 2. Abrir o Notebook Principal
Navegue até: `src/analise_dw_coffee_llm.ipynb`

### 3. Executar Análise
1. **Execute todas as células** usando `Cell → Run All`
2. **Ou execute passo a passo** usando `Shift + Enter`

### 4. Requisitos Importantes
- **Certifique-se** que o pipeline da API foi executado primeiro
- **Verifique** se a tabela `dw_coffee` existe no PostgreSQL
- **Aguarde** o carregamento completo dos dados (3.547 registros)

## 🤖 Algoritmos e Técnicas de LLM Utilizadas

### 1. Análise Semântica de Padrões
- **Técnica**: Processamento de linguagem natural aplicado aos dados
- **Objetivo**: Identificar padrões ocultos nas vendas de café
- **Implementação**: Análise de correlações usando algoritmos de clustering semântico

### 2. Simulação de LLM para Insights
- **Técnica**: Geração automática de insights baseada em regras inteligentes
- **Algoritmo**: Sistema de recomendação baseado em padrões temporais
- **Aplicação**: Análise de tendências de vendas por período e categoria

### 3. Análise Preditiva Baseada em Contexto
- **Técnica**: Algoritmos de análise temporal com contexto semântico
- **Implementação**: Uso de séries temporais com análise de sentimento simulada
- **Objetivo**: Prever comportamentos de compra baseados em padrões históricos

### 4. Processamento de Texto Estruturado
- **Algoritmo**: NLP aplicado aos nomes de produtos e categorias
- **Técnica**: Tokenização e análise de frequência
- **Uso**: Extração de insights sobre preferências de produtos

### 5. Clustering Inteligente de Clientes
- **Técnica**: Segmentação baseada em comportamento de compra
- **Algoritmo**: K-means adaptado com pesos semânticos
- **Objetivo**: Identificar grupos de clientes com padrões similares

### 6. Análise de Sentimento Simulada
- **Técnica**: Classificação de produtos por popularidade e sazonalidade
- **Implementação**: Sistema de pontuação baseado em vendas e frequência
- **Aplicação**: Ranking de produtos mais "desejados" pelo mercado

## 📚 Referências dos Algoritmos

### Análise de Dados e Data Science
1. **McKinney, W.** (2022). *Python for Data Analysis: Data Wrangling with pandas, NumPy, and Jupyter*. O'Reilly Media.

2. **VanderPlas, J.** (2016). *Python Data Science Handbook: Essential Tools for Working with Data*. O'Reilly Media.

### Técnicas de LLM e NLP
3. **Jurafsky, D., & Martin, J. H.** (2023). *Speech and Language Processing*. 3rd Edition. Pearson.

4. **Khurana, D., et al.** (2023). "Natural Language Processing: State of The Art, Current Trends and Challenges." *Multimedia Tools and Applications*, 82(3), 3713-3744.

### Machine Learning e Clustering
5. **Hastie, T., Tibshirani, R., & Friedman, J.** (2017). *The Elements of Statistical Learning: Data Mining, Inference, and Prediction*. 2nd Edition. Springer.

6. **Scikit-learn Developers** (2023). *Scikit-learn: Machine Learning in Python*. Journal of Machine Learning Research.

### Visualização de Dados
7. **Cairo, A.** (2019). *How Charts Lie: Getting Smarter about Visual Information*. W. W. Norton & Company.

8. **Plotly Technologies Inc.** (2023). *Plotly Python Graphing Library Documentation*. https://plotly.com/python/

### Data Warehousing
9. **Kimball, R., & Ross, M.** (2013). *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling*. 3rd Edition. Wiley.

10. **Inmon, W. H.** (2005). *Building the Data Warehouse*. 4th Edition. Wiley.

### APIs e Desenvolvimento Web
11. **FastAPI Documentation** (2023). *FastAPI: Modern, fast (high-performance), web framework for building APIs*. https://fastapi.tiangolo.com/

12. **PostgreSQL Global Development Group** (2023). *PostgreSQL Documentation*. https://www.postgresql.org/docs/

---

## 🎯 Funcionalidades Principais

- ✅ **Pipeline ETL Completo** - Extração, transformação e carregamento automático
- ✅ **Data Warehouse** - Estrutura otimizada para análise
- ✅ **API REST** - Endpoints para executar o pipeline
- ✅ **Análise com LLM** - Insights inteligentes automatizados  
- ✅ **Visualizações Interativas** - Gráficos Plotly responsivos
- ✅ **Containerização** - Deploy fácil com Podman
- ✅ **Documentação Swagger** - API autodocumentada
- ✅ **Jupyter Notebook** - Ambiente interativo de análise

## 📊 Dados do Projeto

- **Dataset Original**: [Coffee Sales - Kaggle](https://www.kaggle.com/datasets/minahilfatima12328/daily-coffee-transactions/data)
- **Registros**: 3.547 vendas processadas
- **Tabelas**: `coffee_sales` (raw) → `dw_coffee` (processada)
- **Período**: Dados históricos de vendas de café
- **Qualidade**: Dados limpos, sem outliers e valores ausentes

## 🆘 Suporte e Troubleshooting

### Problemas Comuns

1. **Erro de conexão com PostgreSQL**: 
   - Aguarde a inicialização completa dos containers
   - Verifique se a porta 5432 não está em uso

2. **Notebook não carrega dados**:
   - Execute o pipeline da API primeiro
   - Verifique se a tabela `dw_coffee` existe

3. **API não responde**:
   - Verifique se o container está rodando: `podman ps`
   - Consulte os logs: `podman logs coffee-api`

### Logs do Sistema
```bash
# Ver logs da API
podman logs coffee-api

# Ver logs do PostgreSQL  
podman logs techchallenge03-postgres

# Ver logs em tempo real
podman logs -f coffee-api
```

---

**Desenvolvido para Tech Challenge 03** 🚀  
*Data Warehouse, APIs e LLM para análise inteligente de dados*

