#!/bin/bash
# Script de inicialização da API e Jupyter Notebook
# Tech Challenge 03

echo "🚀 Iniciando Tech Challenge 03..."

# Criar diretórios internos se não existirem
mkdir -p /app/log /app/data

# Definir permissões corretas
chmod 755 /app/log /app/data

echo "✅ Configuração inicial concluída. Iniciando serviços..."

# Definir PYTHONPATH
export PYTHONPATH="/app:$PYTHONPATH"

# Mudar para o diretório correto
cd /app

# Função para iniciar Jupyter Notebook
start_jupyter() {
    echo "🔬 Iniciando Jupyter Notebook na porta 8888..."
    jupyter notebook \
        --ip=0.0.0.0 \
        --port=8888 \
        --no-browser \
        --allow-root \
        --NotebookApp.token='' \
        --NotebookApp.password='' \
        --NotebookApp.allow_origin='*' \
        --NotebookApp.default_url='/notebooks/analise_dw_coffee_llm.ipynb' \
        --NotebookApp.open_browser=False \
        --notebook-dir=/app/src \
        > /app/log/jupyter.log 2>&1 &
    
    echo "✅ Jupyter Notebook iniciado em background"
    echo "📓 Notebook padrão: analise_dw_coffee_llm.ipynb"
}

# Função para iniciar API
start_api() {
    echo "🌐 Iniciando API na porta 8000..."
    python -c "import sys; import src.api; print('✅ Módulo API importado com sucesso!')" || echo "❌ Erro ao importar módulo"
    
    python -m uvicorn src.api:app \
        --host 0.0.0.0 \
        --port 8000 \
        --log-level info
}

# Iniciar Jupyter Notebook em background
start_jupyter

# Aguardar um momento para o Jupyter inicializar
sleep 3

# Iniciar API (processo principal)
start_api