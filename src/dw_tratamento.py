#!/usr/bin/env python3
"""
Data Warehouse - Tratamento dos dados Coffee Sales
Lê a tabela coffee_sales, aplica transformações e cria a tabela dw_coffee
"""

import sys
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from datetime import datetime, time
import warnings
warnings.filterwarnings('ignore')

# Configuração de logging - apenas terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataWarehouseTratamento:
    """Classe para tratamento e criação do Data Warehouse Coffee Sales"""
    
    def __init__(self):
        """
        Inicializa a conexão com PostgreSQL usando variáveis do .env
        """
        # Carregar variáveis de ambiente
        env_path = Path('../.env')
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Carregando .env de: {env_path.absolute()}")
        else:
            load_dotenv()
            logger.info("Carregando .env do diretório atual")
        
        # Configurações do PostgreSQL do .env
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'techchallenge03'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'admin123')
        }
        
        # Nomes das tabelas
        self.source_table = 'coffee_sales'
        self.target_table = 'dw_coffee'
        
        logger.info(f"Configuração PostgreSQL: {self.db_config['user']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
    
    def create_connection_string(self) -> str:
        """
        Cria string de conexão para SQLAlchemy
        
        Returns:
            str: String de conexão PostgreSQL
        """
        return f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
    
    def test_connection(self) -> bool:
        """
        Testa a conexão com o PostgreSQL
        
        Returns:
            bool: True se conexão bem-sucedida
        """
        try:
            logger.info("🔌 Testando conexão com PostgreSQL...")
            
            # Testar conexão direta com psycopg2
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
            logger.info(f"✅ Conexão bem-sucedida!")
            logger.info(f"   📊 PostgreSQL: {version}")
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na conexão: {str(e)}")
            return False
    
    def verify_source_table(self) -> bool:
        """
        Verifica se a tabela de origem existe
        
        Returns:
            bool: True se tabela existe
        """
        try:
            logger.info(f"📋 Verificando tabela de origem: {self.source_table}")
            
            engine = create_engine(self.create_connection_string())
            inspector = inspect(engine)
            
            if self.source_table not in inspector.get_table_names():
                logger.error(f"❌ Tabela {self.source_table} não encontrada!")
                logger.info("💡 Execute primeiro: python persistencia.py")
                return False
            
            # Verificar quantidade de registros
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.source_table}"))
                count = result.scalar()
                logger.info(f"✅ Tabela {self.source_table} encontrada com {count:,} registros")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar tabela de origem: {str(e)}")
            return False
    
    def extract_data(self) -> pd.DataFrame:
        """
        Extrai dados da tabela coffee_sales
        
        Returns:
            pd.DataFrame: Dados extraídos
        """
        try:
            logger.info(f"📥 Extraindo dados de {self.source_table}...")
            
            engine = create_engine(self.create_connection_string())
            
            # Extrair todos os dados
            query = f"SELECT * FROM {self.source_table} ORDER BY date, time"
            df = pd.read_sql_query(query, engine)
            
            logger.info(f"✅ Dados extraídos com sucesso!")
            logger.info(f"   📏 Dimensões: {df.shape[0]:,} linhas × {df.shape[1]} colunas")
            logger.info(f"   📋 Colunas: {list(df.columns)}")
            
            # Mostrar tipos de dados atuais
            logger.info("🔍 Tipos de dados atuais:")
            for col, dtype in df.dtypes.items():
                logger.info(f"   {col}: {dtype}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Erro na extração: {str(e)}")
            raise
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformações nos dados
        
        Args:
            df: DataFrame original
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        try:
            logger.info("🔄 Iniciando transformações dos dados...")
            
            # Criar cópia para não alterar original
            df_transformed = df.copy()
            initial_count = len(df_transformed)
            
            # 1. Transformar coluna date para formato YYYY-MM-DD
            logger.info("📅 Transformando coluna 'date'...")
            if 'date' in df_transformed.columns:
                # Converter para datetime se ainda não estiver
                df_transformed['date'] = pd.to_datetime(df_transformed['date'])
                # Formatar como YYYY-MM-DD
                df_transformed['date'] = df_transformed['date'].dt.strftime('%Y-%m-%d')
                logger.info("   ✅ Coluna 'date' formatada como YYYY-MM-DD")
            
            # 2. Transformar coluna time para formato HH:MM:SS
            logger.info("⏰ Transformando coluna 'time'...")
            if 'time' in df_transformed.columns:
                def format_time(time_str):
                    """Formata time para HH:MM:SS"""
                    if pd.isna(time_str):
                        return None
                    try:
                        # Se já é um objeto time
                        if isinstance(time_str, time):
                            return time_str.strftime('%H:%M:%S')
                        
                        # Se é string, processar
                        time_str = str(time_str)
                        
                        # Remover microssegundos se existirem
                        if '.' in time_str:
                            time_str = time_str.split('.')[0]
                        
                        # Garantir formato HH:MM:SS
                        parts = time_str.split(':')
                        if len(parts) == 3:
                            return f"{int(parts[0]):02d}:{int(parts[1]):02d}:{int(parts[2]):02d}"
                        else:
                            return None
                    except:
                        return None
                
                df_transformed['time'] = df_transformed['time'].apply(format_time)
                valid_times = df_transformed['time'].notna().sum()
                logger.info(f"   ✅ Coluna 'time' formatada como HH:MM:SS ({valid_times:,} válidos)")
            
            # 3. Remover dados faltantes
            logger.info("🧹 Removendo dados faltantes...")
            
            # Verificar valores nulos por coluna
            null_counts = df_transformed.isnull().sum()
            total_nulls = null_counts.sum()
            
            if total_nulls > 0:
                logger.info("   📊 Valores nulos por coluna:")
                for col, count in null_counts[null_counts > 0].items():
                    logger.info(f"     {col}: {count:,} nulos")
                
                # Remover linhas com valores nulos
                df_transformed = df_transformed.dropna()
                removed_nulls = initial_count - len(df_transformed)
                logger.info(f"   ✅ Removidos {removed_nulls:,} registros com dados faltantes")
            else:
                logger.info("   ✅ Nenhum valor nulo encontrado")
            
            # 4. Remover outliers na coluna 'money'
            logger.info("📊 Removendo outliers da coluna 'money'...")
            
            if 'money' in df_transformed.columns:
                # Calcular quartis e IQR
                Q1 = df_transformed['money'].quantile(0.25)
                Q3 = df_transformed['money'].quantile(0.75)
                IQR = Q3 - Q1
                
                # Definir limites para outliers
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Identificar outliers
                outliers_mask = (df_transformed['money'] < lower_bound) | (df_transformed['money'] > upper_bound)
                outliers_count = outliers_mask.sum()
                
                logger.info(f"   📈 Estatísticas originais:")
                logger.info(f"     Q1: ${Q1:.2f}, Q3: ${Q3:.2f}, IQR: ${IQR:.2f}")
                logger.info(f"     Limites: ${lower_bound:.2f} - ${upper_bound:.2f}")
                logger.info(f"     Outliers encontrados: {outliers_count:,}")
                
                if outliers_count > 0:
                    # Mostrar alguns exemplos de outliers
                    outliers_sample = df_transformed[outliers_mask]['money'].head(5)
                    logger.info(f"   🚨 Exemplos de outliers: {outliers_sample.tolist()}")
                    
                    # Remover outliers
                    df_transformed = df_transformed[~outliers_mask]
                    logger.info(f"   ✅ Removidos {outliers_count:,} outliers")
                else:
                    logger.info("   ✅ Nenhum outlier encontrado")
            
            # 5. Validações adicionais
            logger.info("🔍 Aplicando validações adicionais...")
            
            # Remover registros com valores negativos em 'money'
            if 'money' in df_transformed.columns:
                negative_money = (df_transformed['money'] <= 0).sum()
                if negative_money > 0:
                    df_transformed = df_transformed[df_transformed['money'] > 0]
                    logger.info(f"   ✅ Removidos {negative_money} registros com valores negativos")
            
            # Remover registros com hour_of_day inválidos
            if 'hour_of_day' in df_transformed.columns:
                invalid_hours = ((df_transformed['hour_of_day'] < 0) | (df_transformed['hour_of_day'] > 23)).sum()
                if invalid_hours > 0:
                    df_transformed = df_transformed[(df_transformed['hour_of_day'] >= 0) & (df_transformed['hour_of_day'] <= 23)]
                    logger.info(f"   ✅ Removidos {invalid_hours} registros com horas inválidas")
            
            # 6. Estatísticas finais
            final_count = len(df_transformed)
            removed_total = initial_count - final_count
            retention_rate = (final_count / initial_count) * 100
            
            logger.info("📊 Resumo das transformações:")
            logger.info(f"   📈 Registros iniciais: {initial_count:,}")
            logger.info(f"   📉 Registros removidos: {removed_total:,}")
            logger.info(f"   ✅ Registros finais: {final_count:,}")
            logger.info(f"   📊 Taxa de retenção: {retention_rate:.1f}%")
            
            # Estatísticas da coluna money após limpeza
            if 'money' in df_transformed.columns:
                logger.info("💰 Estatísticas finais da coluna 'money':")
                logger.info(f"   Média: ${df_transformed['money'].mean():.2f}")
                logger.info(f"   Mediana: ${df_transformed['money'].median():.2f}")
                logger.info(f"   Desvio padrão: ${df_transformed['money'].std():.2f}")
                logger.info(f"   Mín/Máx: ${df_transformed['money'].min():.2f} / ${df_transformed['money'].max():.2f}")
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"❌ Erro nas transformações: {str(e)}")
            raise
    
    def create_dw_table(self, df: pd.DataFrame) -> bool:
        """
        Cria a tabela dw_coffee no PostgreSQL
        
        Args:
            df: DataFrame com os dados transformados
            
        Returns:
            bool: True se tabela criada com sucesso
        """
        try:
            logger.info(f"🏗️  Criando tabela Data Warehouse: {self.target_table}")
            
            engine = create_engine(self.create_connection_string())
            
            # Verificar se tabela já existe
            inspector = inspect(engine)
            if self.target_table in inspector.get_table_names():
                logger.warning(f"⚠️  Tabela '{self.target_table}' já existe!")
                
                response = input("Deseja recriar a tabela? (s/N): ")
                if response.lower() in ['s', 'sim', 'y', 'yes']:
                    logger.info(f"🗑️  Removendo tabela existente...")
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {self.target_table}"))
                        conn.commit()
                else:
                    logger.info("📋 Usando tabela existente")
                    return True
            
            # Criar SQL CREATE TABLE personalizado
            create_sql = f"""
            CREATE TABLE {self.target_table} (
                id SERIAL PRIMARY KEY,
                hour_of_day INTEGER,
                cash_type VARCHAR(50),
                money DECIMAL(10,2),
                coffee_name VARCHAR(255),
                time_of_day VARCHAR(50),
                weekday VARCHAR(20),
                month_name VARCHAR(20),
                weekdaysort INTEGER,
                monthsort INTEGER,
                date DATE,
                time TIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            logger.info(f"✅ Tabela '{self.target_table}' criada com sucesso!")
            
            # Criar índices para melhor performance
            indices_sql = [
                f"CREATE INDEX idx_{self.target_table}_date ON {self.target_table}(date);",
                f"CREATE INDEX idx_{self.target_table}_coffee_name ON {self.target_table}(coffee_name);",
                f"CREATE INDEX idx_{self.target_table}_time_of_day ON {self.target_table}(time_of_day);",
                f"CREATE INDEX idx_{self.target_table}_money ON {self.target_table}(money);"
            ]
            
            with engine.connect() as conn:
                for idx_sql in indices_sql:
                    try:
                        conn.execute(text(idx_sql))
                        conn.commit()
                    except Exception as idx_err:
                        logger.warning(f"⚠️  Erro ao criar índice: {idx_err}")
            
            logger.info("📋 Índices criados para melhor performance")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar tabela DW: {str(e)}")
            return False
    
    def load_data(self, df: pd.DataFrame) -> bool:
        """
        Carrega os dados transformados na tabela dw_coffee
        
        Args:
            df: DataFrame com dados transformados
            
        Returns:
            bool: True se inserção bem-sucedida
        """
        try:
            logger.info(f"📥 Carregando {len(df):,} registros na tabela '{self.target_table}'...")
            
            engine = create_engine(self.create_connection_string())
            
            # Preparar DataFrame para inserção (remover colunas que não existem na tabela)
            df_to_load = df.copy()
            
            # Remover coluna 'id' se existir (será auto-gerada)
            if 'id' in df_to_load.columns:
                df_to_load = df_to_load.drop('id', axis=1)
            
            # Inserir dados em lotes
            batch_size = 500
            total_batches = (len(df_to_load) + batch_size - 1) // batch_size
            
            for i in range(0, len(df_to_load), batch_size):
                batch = df_to_load.iloc[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"   📦 Carregando lote {batch_num}/{total_batches} ({len(batch)} registros)")
                
                batch.to_sql(
                    self.target_table,
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            # Verificar total de registros carregados
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.target_table}"))
                count = result.scalar()
                
            logger.info(f"✅ Carregamento concluído!")
            logger.info(f"   📊 Total de registros na tabela DW: {count:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro no carregamento: {str(e)}")
            return False
    
    def verify_dw_data(self) -> bool:
        """
        Verifica os dados carregados na tabela DW
        
        Returns:
            bool: True se verificação bem-sucedida
        """
        try:
            logger.info(f"🔍 Verificando dados na tabela '{self.target_table}'...")
            
            engine = create_engine(self.create_connection_string())
            
            with engine.connect() as conn:
                # Estatísticas gerais
                stats_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT coffee_name) as unique_coffees,
                    AVG(money)::NUMERIC(10,2) as avg_price,
                    SUM(money)::NUMERIC(10,2) as total_revenue
                FROM {self.target_table}
                """
                
                result = conn.execute(text(stats_query))
                stats = result.fetchone()
                
                logger.info("📊 Estatísticas da tabela DW:")
                logger.info(f"   📈 Total de registros: {stats[0]:,}")
                logger.info(f"   📅 Período: {stats[1]} a {stats[2]}")
                logger.info(f"   ☕ Tipos de café únicos: {stats[3]}")
                logger.info(f"   💰 Preço médio: ${stats[4]}")
                logger.info(f"   💵 Receita total: ${stats[5]:,}")
                
                # Verificar qualidade dos dados
                quality_query = f"""
                SELECT 
                    COUNT(*) - COUNT(date) as null_dates,
                    COUNT(*) - COUNT(time) as null_times,
                    COUNT(*) - COUNT(money) as null_money,
                    COUNT(*) - COUNT(coffee_name) as null_coffee_names
                FROM {self.target_table}
                """
                
                quality_result = conn.execute(text(quality_query))
                quality = quality_result.fetchone()
                
                logger.info("🔍 Qualidade dos dados:")
                logger.info(f"   📅 Datas nulas: {quality[0]}")
                logger.info(f"   ⏰ Times nulos: {quality[1]}")
                logger.info(f"   💰 Valores nulos: {quality[2]}")
                logger.info(f"   ☕ Nomes nulos: {quality[3]}")
                
                # Amostra dos dados
                sample_query = f"""
                SELECT date, time, coffee_name, money, time_of_day
                FROM {self.target_table}
                ORDER BY date, time
                LIMIT 5
                """
                
                sample_result = conn.execute(text(sample_query))
                
                logger.info("📋 Amostra dos dados DW:")
                for i, row in enumerate(sample_result, 1):
                    logger.info(f"   {i}: {row[0]} {row[1]} | {row[2]} | ${row[3]} | {row[4]}")
            
            logger.info("✅ Verificação da tabela DW concluída com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na verificação DW: {str(e)}")
            return False


def main():
    """Função principal para execução do processo ETL do Data Warehouse"""
    logger.info("=" * 80)
    logger.info("🏗️  TECH CHALLENGE 03 - DATA WAREHOUSE TRATAMENTO")
    logger.info("=" * 80)
    
    try:
        # Criar instância da classe de tratamento
        dw = DataWarehouseTratamento()
        
        # 1. Testar conexão
        if not dw.test_connection():
            logger.error("❌ Falha na conexão com PostgreSQL")
            return 1
        
        # 2. Verificar tabela de origem
        if not dw.verify_source_table():
            logger.error("❌ Tabela de origem não encontrada")
            return 1
        
        # 3. Extrair dados
        df_raw = dw.extract_data()
        
        # 4. Transformar dados
        df_transformed = dw.transform_data(df_raw)
        
        # 5. Criar tabela DW
        if not dw.create_dw_table(df_transformed):
            logger.error("❌ Falha na criação da tabela DW")
            return 1
        
        # 6. Carregar dados
        if not dw.load_data(df_transformed):
            logger.error("❌ Falha no carregamento dos dados")
            return 1
        
        # 7. Verificar dados carregados
        if not dw.verify_dw_data():
            logger.error("❌ Falha na verificação dos dados DW")
            return 1
        
        logger.info("=" * 80)
        logger.info("✅ PROCESSO ETL DO DATA WAREHOUSE CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 80)
        logger.info("💡 Para consultar os dados:")
        logger.info("   docker-compose exec postgres psql -U admin -d techchallenge03")
        logger.info("   SELECT * FROM dw_coffee LIMIT 10;")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("🛑 Processo interrompido pelo usuário")
        return 1
        
    except Exception as e:
        logger.error(f"❌ Erro inesperado: {str(e)}")
        logger.exception("Detalhes do erro:")
        return 1


if __name__ == "__main__":
    """Ponto de entrada do script"""
    exit_code = main()
    sys.exit(exit_code)