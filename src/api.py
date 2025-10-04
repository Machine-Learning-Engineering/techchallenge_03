#!/usr/bin/env python3
"""
API REST com Swagger para Tech Challenge 03
Expõe endpoints para invocar data_upload.py, persistencia.py e dw_tratamento.py
"""

import sys
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import os

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Importar módulos locais
try:
    from data_upload import SpotifyDataDownloader
    from persistencia import CoffeeSalesPersistencia
    from dw_tratamento import DataWarehouseTratamento
except ImportError as e:
    logging.warning(f"Não foi possível importar módulos locais: {e}")
    SpotifyDataDownloader = None
    CoffeeSalesPersistencia = None
    DataWarehouseTratamento = None

# Configuração de logging - apenas terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('api')

# Configuração da API
app = FastAPI(
    title="Tech Challenge 03 - Coffee Sales API",
    description="""
    ## API REST para processamento de dados Coffee Sales
    
    Esta API permite executar todo o pipeline de dados do Tech Challenge 03:
    
    ### Funcionalidades:
    
    * **📥 Data Upload**: Download do dataset do Kaggle
    * **💾 Persistência**: Carregamento dos dados no PostgreSQL
    * **🏗️ Data Warehouse**: Tratamento e criação do DW
    
    ### Pipeline Completo:
    1. `POST /data-upload/` - Baixa os dados do Kaggle
    2. `POST /persistencia/` - Carrega dados no PostgreSQL (tabela coffee_sales)
    3. `POST /dw-tratamento/` - Cria Data Warehouse (tabela dw_coffee)
    
    ### Execução Automatizada:
    * `POST /pipeline/execute` - Executa todo o pipeline automaticamente
    """,
    version="1.0.0",
    contact={
        "name": "Tech Challenge 03",
        "url": "https://github.com/Machine-Learning-Engineering/techchallenge_03",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# Configuração CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Armazenamento de tarefas em execução (em produção usar Redis ou similar)
running_tasks: Dict[str, Dict[str, Any]] = {}

# Modelos Pydantic para requests/responses
class DataUploadRequest(BaseModel):
    """Request para download de dados"""
    force_download: bool = Field(False, description="Forçar download mesmo se já existir")
    copy_to_local: bool = Field(True, description="Copiar para diretório local")

class DataUploadResponse(BaseModel):
    """Response do download de dados"""
    task_id: str = Field(..., description="ID da tarefa")
    status: str = Field(..., description="Status inicial")
    message: str = Field(..., description="Mensagem")

class PersistenciaRequest(BaseModel):
    """Request para persistência de dados"""
    recreate_table: bool = Field(False, description="Recriar tabela se já existir")
    batch_size: int = Field(1000, description="Tamanho do lote para inserção")

class PersistenciaResponse(BaseModel):
    """Response da persistência"""
    task_id: str = Field(..., description="ID da tarefa")
    status: str = Field(..., description="Status inicial")
    message: str = Field(..., description="Mensagem")

class DWTratamentoRequest(BaseModel):
    """Request para tratamento DW"""
    recreate_dw: bool = Field(False, description="Recriar Data Warehouse se já existir")
    apply_transformations: bool = Field(True, description="Aplicar transformações aos dados")

class DWTratamentoResponse(BaseModel):
    """Response do tratamento DW"""
    task_id: str = Field(..., description="ID da tarefa")
    status: str = Field(..., description="Status inicial")
    message: str = Field(..., description="Mensagem")

class PipelineRequest(BaseModel):
    """Request para execução completa do pipeline"""
    force_download: bool = Field(False, description="Forçar download de dados")
    recreate_tables: bool = Field(False, description="Recriar tabelas se já existirem")

class PipelineResponse(BaseModel):
    """Response da execução do pipeline"""
    task_id: str = Field(..., description="ID da tarefa do pipeline")
    status: str = Field(..., description="Status inicial")
    message: str = Field(..., description="Mensagem")
    steps: List[str] = Field(..., description="Lista de etapas do pipeline")

# Funções auxiliares
def generate_task_id(prefix: str) -> str:
    """Gera um ID único para tarefa"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}"

def update_task_status(task_id: str, status: str, success: Optional[bool] = None, 
                      message: Optional[str] = None, details: Optional[Dict] = None):
    """Atualiza status de uma tarefa"""
    if task_id in running_tasks:
        running_tasks[task_id]["status"] = status
        running_tasks[task_id]["message"] = message
        running_tasks[task_id]["details"] = details
        
        if success is not None:
            running_tasks[task_id]["success"] = success
            running_tasks[task_id]["completed_at"] = datetime.now()

async def run_subprocess(command: List[str], task_id: str, description: str) -> bool:
    """Executa um subprocess de forma assíncrona"""
    try:
        logger.info(f"[{task_id}] Executando: {' '.join(command)}")
        update_task_status(task_id, "running", message=f"Executando {description}...")
        
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=Path(__file__).parent
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            logger.info(f"[{task_id}] {description} concluído com sucesso")
            update_task_status(task_id, "completed", success=True, 
                             message=f"{description} concluído com sucesso",
                             details={"stdout": stdout.decode(), "stderr": stderr.decode()})
            return True
        else:
            logger.error(f"[{task_id}] {description} falhou: {stderr.decode()}")
            update_task_status(task_id, "failed", success=False, 
                             message=f"{description} falhou",
                             details={"stdout": stdout.decode(), "stderr": stderr.decode(), 
                                    "return_code": process.returncode})
            return False
            
    except Exception as e:
        logger.error(f"[{task_id}] Erro ao executar {description}: {str(e)}")
        update_task_status(task_id, "failed", success=False, 
                         message=f"Erro ao executar {description}: {str(e)}")
        return False

# Endpoints da API

@app.post("/data-upload/", response_model=DataUploadResponse, tags=["Data Upload"])
async def data_upload(request: DataUploadRequest, background_tasks: BackgroundTasks):
    """
    Executa download do dataset do Kaggle
    
    Baixa o dataset de vendas de café do Kaggle usando o módulo data_upload.py
    """
    task_id = generate_task_id("data_upload")
    
    # Registrar tarefa
    running_tasks[task_id] = {
        "task_id": task_id,
        "status": "started",
        "started_at": datetime.now(),
        "completed_at": None,
        "success": None,
        "message": "Iniciando download do dataset...",
        "details": {"request": request.dict()}
    }
    
    # Executar em background
    async def execute_data_upload():
        try:
            command = [
                sys.executable, "data_upload.py",
                "--force-download" if request.force_download else "",
                "--copy-to-local" if request.copy_to_local else ""
            ]
            command = [cmd for cmd in command if cmd]  # Remove strings vazias
            
            await run_subprocess(command, task_id, "Download do dataset")
            
        except Exception as e:
            logger.error(f"[{task_id}] Erro no data upload: {str(e)}")
            update_task_status(task_id, "failed", success=False, message=str(e))
    
    background_tasks.add_task(execute_data_upload)
    
    return DataUploadResponse(
        task_id=task_id,
        status="started",
        message="Download do dataset iniciado em background"
    )

@app.post("/persistencia/", response_model=PersistenciaResponse, tags=["Persistência"])
async def persistencia(request: PersistenciaRequest, background_tasks: BackgroundTasks):
    """
    Executa persistência dos dados no PostgreSQL
    
    Carrega os dados do CSV para a tabela coffee_sales no PostgreSQL
    """
    task_id = generate_task_id("persistencia")
    
    # Registrar tarefa
    running_tasks[task_id] = {
        "task_id": task_id,
        "status": "started",
        "started_at": datetime.now(),
        "completed_at": None,
        "success": None,
        "message": "Iniciando persistência dos dados...",
        "details": {"request": request.dict()}
    }
    
    # Executar em background
    async def execute_persistencia():
        try:
            command = [sys.executable, "persistencia.py"]
            await run_subprocess(command, task_id, "Persistência dos dados")
            
        except Exception as e:
            logger.error(f"[{task_id}] Erro na persistência: {str(e)}")
            update_task_status(task_id, "failed", success=False, message=str(e))
    
    background_tasks.add_task(execute_persistencia)
    
    return PersistenciaResponse(
        task_id=task_id,
        status="started",
        message="Persistência dos dados iniciada em background"
    )

@app.post("/dw-tratamento/", response_model=DWTratamentoResponse, tags=["Data Warehouse"])
async def dw_tratamento(request: DWTratamentoRequest, background_tasks: BackgroundTasks):
    """
    Executa tratamento e criação do Data Warehouse
    
    Processa a tabela coffee_sales e cria a tabela dw_coffee com transformações
    """
    task_id = generate_task_id("dw_tratamento")
    
    # Registrar tarefa
    running_tasks[task_id] = {
        "task_id": task_id,
        "status": "started",
        "started_at": datetime.now(),
        "completed_at": None,
        "success": None,
        "message": "Iniciando tratamento do Data Warehouse...",
        "details": {"request": request.dict()}
    }
    
    # Executar em background
    async def execute_dw_tratamento():
        try:
            command = [sys.executable, "dw_tratamento.py"]
            await run_subprocess(command, task_id, "Tratamento do Data Warehouse")
            
        except Exception as e:
            logger.error(f"[{task_id}] Erro no tratamento DW: {str(e)}")
            update_task_status(task_id, "failed", success=False, message=str(e))
    
    background_tasks.add_task(execute_dw_tratamento)
    
    return DWTratamentoResponse(
        task_id=task_id,
        status="started",
        message="Tratamento do Data Warehouse iniciado em background"
    )

@app.post("/pipeline/execute", response_model=PipelineResponse, tags=["Pipeline"])
async def execute_pipeline(request: PipelineRequest, background_tasks: BackgroundTasks):
    """
    Executa todo o pipeline de dados automaticamente
    
    Executa em sequência:
    1. Download do dataset (data_upload.py)
    2. Persistência no PostgreSQL (persistencia.py) 
    3. Tratamento do Data Warehouse (dw_tratamento.py)
    """
    task_id = generate_task_id("pipeline")
    
    steps = [
        "1. Download do dataset",
        "2. Persistência no PostgreSQL", 
        "3. Tratamento do Data Warehouse"
    ]
    
    # Registrar tarefa
    running_tasks[task_id] = {
        "task_id": task_id,
        "status": "started",
        "started_at": datetime.now(),
        "completed_at": None,
        "success": None,
        "message": "Iniciando pipeline completo...",
        "details": {"request": request.dict(), "steps": steps}
    }
    
    # Executar pipeline completo em background
    async def execute_full_pipeline():
        try:
            # Etapa 1: Data Upload
            update_task_status(task_id, "running", message="Etapa 1/3: Download do dataset...")
            cmd1 = [sys.executable, "data_upload.py"]
            if request.force_download:
                cmd1.append("--force-download")
            cmd1.append("--copy-to-local")
            
            success1 = await run_subprocess(cmd1, f"{task_id}_step1", "Download do dataset")
            if not success1:
                update_task_status(task_id, "failed", success=False, 
                                 message="Pipeline falhou na etapa 1: Download do dataset")
                return
            
            # Etapa 2: Persistência
            update_task_status(task_id, "running", message="Etapa 2/3: Persistência dos dados...")
            cmd2 = [sys.executable, "persistencia.py"]
            success2 = await run_subprocess(cmd2, f"{task_id}_step2", "Persistência dos dados")
            if not success2:
                update_task_status(task_id, "failed", success=False, 
                                 message="Pipeline falhou na etapa 2: Persistência dos dados")
                return
            
            # Etapa 3: Data Warehouse
            update_task_status(task_id, "running", message="Etapa 3/3: Tratamento do Data Warehouse...")
            cmd3 = [sys.executable, "dw_tratamento.py"]
            success3 = await run_subprocess(cmd3, f"{task_id}_step3", "Tratamento do Data Warehouse")
            if not success3:
                update_task_status(task_id, "failed", success=False, 
                                 message="Pipeline falhou na etapa 3: Tratamento do Data Warehouse")
                return
            
            # Pipeline completo com sucesso
            update_task_status(task_id, "completed", success=True, 
                             message="Pipeline completo executado com sucesso!",
                             details={"completed_steps": 3, "total_steps": 3})
            
        except Exception as e:
            logger.error(f"[{task_id}] Erro no pipeline: {str(e)}")
            update_task_status(task_id, "failed", success=False, 
                             message=f"Erro no pipeline: {str(e)}")
    
    background_tasks.add_task(execute_full_pipeline)
    
    return PipelineResponse(
        task_id=task_id,
        status="started",
        message="Pipeline completo iniciado em background",
        steps=steps
    )

# Função principal para executar a API
def main():
    """Função principal para executar a API"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Tech Challenge 03 - Coffee Sales API")
    parser.add_argument("--host", default="0.0.0.0", help="Host para bind da API")
    parser.add_argument("--port", type=int, default=8000, help="Porta para bind da API")
    parser.add_argument("--reload", action="store_true", help="Modo reload para desenvolvimento")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("🚀 TECH CHALLENGE 03 - COFFEE SALES API")
    logger.info("=" * 70)
    logger.info(f"📡 Host: {args.host}")
    logger.info(f"🔌 Porta: {args.port}")
    logger.info(f"🔄 Reload: {args.reload}")
    logger.info(f"📋 Log Level: {args.log_level}")
    logger.info("=" * 70)
    logger.info("🌐 Acesse a documentação Swagger em:")
    logger.info(f"   http://{args.host}:{args.port}/docs")
    logger.info("📚 Ou a documentação ReDoc em:")
    logger.info(f"   http://{args.host}:{args.port}/redoc")
    logger.info("=" * 70)
    
    # Executar servidor
    uvicorn.run(
        "api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level
    )

if __name__ == "__main__":
    main()
