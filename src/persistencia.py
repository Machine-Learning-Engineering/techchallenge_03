#!/usr/bin/env python3
"""
Persistência de dados do Coffee Sales no PostgreSQL
Lê o arquivo CSV e armazena no banco de dados usando as credenciais do .env
"""

import sys
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('persistencia.log')
    ]
)
logger = logging.getLogger(__name__)


class CoffeeSalesPersistencia:
    """Classe para gerenciar a persistência dos dados de vendas de café no PostgreSQL"""
    
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
            # Tentar no diretório atual
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
        
        # Caminho do arquivo CSV
        self.csv_path = Path('data/coffee_dataset/Coffe_sales.csv')
        
        # Nome da tabela no PostgreSQL
        self.table_name = 'coffee_sales'
        
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
            logger.info("💡 Verifique se o PostgreSQL está rodando:")
            logger.info("   docker-compose up -d")
            logger.info("   ou ./manage-postgres.sh start")
            return False
    
    def load_csv_data(self) -> pd.DataFrame:
        """
        Carrega os dados do CSV
        
        Returns:
            pd.DataFrame: Dados do coffee sales
        """
        try:
            logger.info(f"📂 Carregando CSV: {self.csv_path}")
            
            if not self.csv_path.exists():
                raise FileNotFoundError(f"Arquivo CSV não encontrado: {self.csv_path}")
            
            # Carregar CSV
            df = pd.read_csv(self.csv_path)
            logger.info(f"✅ CSV carregado com sucesso!")
            logger.info(f"   📏 Dimensões: {df.shape[0]} linhas × {df.shape[1]} colunas")
            logger.info(f"   📋 Colunas: {list(df.columns)}")
            
            # Mostrar informações do dataset
            logger.info("📈 Informações do dataset:")
            logger.info(f"   ☕ Total de vendas: {len(df):,}")
            
            if 'money' in df.columns:
                total_revenue = df['money'].sum()
                avg_sale = df['money'].mean()
                logger.info(f"   💰 Receita total: ${total_revenue:,.2f}")
                logger.info(f"   📊 Venda média: ${avg_sale:.2f}")
            
            if 'coffee_name' in df.columns:
                coffee_types = df['coffee_name'].nunique()
                logger.info(f"   ☕ Tipos de café: {coffee_types}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Erro ao carregar CSV: {str(e)}")
            raise
    
    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara e limpa os dados para inserção no PostgreSQL
        
        Args:
            df: DataFrame original
            
        Returns:
            pd.DataFrame: DataFrame preparado
        """
        try:
            logger.info("🔧 Preparando dados para PostgreSQL...")
            
            # Criar cópia para não alterar o original
            df_clean = df.copy()
            
            # Converter colunas de data/hora se existirem
            if 'Date' in df_clean.columns:
                df_clean['Date'] = pd.to_datetime(df_clean['Date'])
                logger.info("   📅 Coluna 'Date' convertida para datetime")
            
            if 'Time' in df_clean.columns:
                # Analisar formatos de tempo presentes
                time_samples = df_clean['Time'].head(10).tolist()
                logger.info(f"   🔍 Amostras de formato de tempo: {time_samples}")
                
                # Verificar se há microssegundos nos dados
                has_microseconds = any('.' in str(time_val) for time_val in time_samples if pd.notna(time_val))
                logger.info(f"   ⏱️  Microssegundos detectados: {has_microseconds}")
                
                try:
                    if has_microseconds:
                        # Converter usando formato inferred (pandas escolhe automaticamente)
                        df_clean['Time'] = pd.to_datetime(df_clean['Time'], infer_datetime_format=True).dt.time
                        logger.info("   ⏰ Coluna 'Time' convertida com inferência automática de formato")
                    else:
                        # Usar formato padrão sem microssegundos
                        df_clean['Time'] = pd.to_datetime(df_clean['Time'], format='%H:%M:%S').dt.time
                        logger.info("   ⏰ Coluna 'Time' convertida para time (formato padrão)")
                        
                except Exception as e:
                    logger.warning(f"   ⚠️  Erro na conversão de tempo: {e}")
                    try:
                        # Fallback final: usar errors='coerce' para converter o que for possível
                        df_clean['Time'] = pd.to_datetime(df_clean['Time'], errors='coerce').dt.time
                        invalid_count = df_clean['Time'].isna().sum()
                        valid_count = len(df_clean) - invalid_count
                        logger.info(f"   ✅ Conversão com coerção: {valid_count:,} válidos, {invalid_count} inválidos")
                        
                        if invalid_count > 0:
                            # Mostrar alguns exemplos de valores problemáticos
                            invalid_samples = df_clean[df_clean['Time'].isna()]['Time'].head(3)
                            logger.warning(f"   🚨 Exemplos de valores problemáticos: {invalid_samples.tolist()}")
                            
                    except Exception as e2:
                        logger.error(f"   ❌ Falha total na conversão de tempo: {e2}")
                        # Manter como string se tudo falhar
                        logger.info("   📝 Mantendo coluna 'Time' como string")
            
            # Limpar nomes de colunas (remover espaços, caracteres especiais)
            original_cols = df_clean.columns.tolist()
            df_clean.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_clean.columns]
            
            if original_cols != df_clean.columns.tolist():
                logger.info("   🔤 Nomes de colunas padronizados")
                for old, new in zip(original_cols, df_clean.columns):
                    if old != new:
                        logger.info(f"     '{old}' -> '{new}'")
            
            # Verificar valores nulos
            null_counts = df_clean.isnull().sum()
            if null_counts.sum() > 0:
                logger.warning("⚠️  Valores nulos encontrados:")
                for col, count in null_counts[null_counts > 0].items():
                    logger.warning(f"     {col}: {count} nulos")
            
            logger.info(f"✅ Dados preparados. Shape final: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            logger.error(f"❌ Erro na preparação dos dados: {str(e)}")
            raise
    
    def create_table(self, df: pd.DataFrame) -> bool:
        """
        Cria a tabela no PostgreSQL baseada na estrutura do DataFrame
        
        Args:
            df: DataFrame com os dados
            
        Returns:
            bool: True se tabela criada com sucesso
        """
        try:
            logger.info(f"🏗️  Criando tabela '{self.table_name}' no PostgreSQL...")
            
            # Criar engine do SQLAlchemy
            engine = create_engine(self.create_connection_string())
            
            # Verificar se tabela já existe
            inspector = inspect(engine)
            if self.table_name in inspector.get_table_names():
                logger.warning(f"⚠️  Tabela '{self.table_name}' já existe!")
                
                response = input("Deseja recriar a tabela? (s/N): ")
                if response.lower() in ['s', 'sim', 'y', 'yes']:
                    logger.info(f"🗑️  Removendo tabela existente...")
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                        conn.commit()
                else:
                    logger.info("📋 Usando tabela existente")
                    return True
            
            # Usar pandas para criar a tabela automaticamente
            # Isso criará a tabela com tipos de dados apropriados
            sample_df = df.head(0)  # DataFrame vazio com só as colunas
            sample_df.to_sql(
                self.table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            logger.info(f"✅ Tabela '{self.table_name}' criada com sucesso!")
            
            # Mostrar estrutura da tabela
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table_name}'
                    ORDER BY ordinal_position
                """))
                
                logger.info("📋 Estrutura da tabela:")
                for row in result:
                    logger.info(f"   {row[0]}: {row[1]} ({'NULL' if row[2] == 'YES' else 'NOT NULL'})")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar tabela: {str(e)}")
            return False
    
    def insert_data(self, df: pd.DataFrame) -> bool:
        """
        Insere os dados na tabela PostgreSQL
        
        Args:
            df: DataFrame com os dados preparados
            
        Returns:
            bool: True se inserção bem-sucedida
        """
        try:
            logger.info(f"📥 Inserindo {len(df):,} registros na tabela '{self.table_name}'...")
            
            # Criar engine do SQLAlchemy
            engine = create_engine(self.create_connection_string())
            
            # Inserir dados em lotes para melhor performance
            batch_size = 1000
            total_batches = (len(df) + batch_size - 1) // batch_size
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                logger.info(f"   📦 Inserindo lote {batch_num}/{total_batches} ({len(batch)} registros)")
                
                batch.to_sql(
                    self.table_name,
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            # Verificar total de registros inseridos
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}"))
                count = result.scalar()
                
            logger.info(f"✅ Inserção concluída!")
            logger.info(f"   📊 Total de registros na tabela: {count:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na inserção: {str(e)}")
            return False
    
    def verify_data(self) -> bool:
        """
        Verifica os dados inseridos na tabela
        
        Returns:
            bool: True se verificação bem-sucedida
        """
        try:
            logger.info(f"🔍 Verificando dados na tabela '{self.table_name}'...")
            
            # Criar engine do SQLAlchemy
            engine = create_engine(self.create_connection_string())
            
            # Estatísticas básicas
            with engine.connect() as conn:
                # Contar registros
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}"))
                total_records = count_result.scalar()
                
                # Primeiros registros
                sample_result = conn.execute(text(f"SELECT * FROM {self.table_name} LIMIT 5"))
                sample_data = sample_result.fetchall()
                columns = sample_result.keys()
                
            logger.info(f"📊 Estatísticas da tabela:")
            logger.info(f"   📈 Total de registros: {total_records:,}")
            logger.info(f"   📋 Colunas: {len(columns)}")
            
            # Mostrar amostra dos dados
            logger.info("📋 Amostra dos dados (5 primeiros registros):")
            for i, row in enumerate(sample_data, 1):
                logger.info(f"   {i}: {dict(zip(columns, row))}")
            
            # Verificações específicas se colunas existirem
            with engine.connect() as conn:
                if 'money' in [col.lower() for col in columns]:
                    revenue_result = conn.execute(text(f"SELECT SUM(money)::numeric(10,2), AVG(money)::numeric(10,2) FROM {self.table_name}"))
                    total_rev, avg_rev = revenue_result.fetchone()
                    logger.info(f"   💰 Receita total: ${total_rev:,}")
                    logger.info(f"   📊 Venda média: ${avg_rev}")
                
                if 'coffee_name' in [col.lower() for col in columns]:
                    coffee_result = conn.execute(text(f"SELECT coffee_name, COUNT(*) FROM {self.table_name} GROUP BY coffee_name ORDER BY COUNT(*) DESC LIMIT 5"))
                    logger.info("   ☕ Top 5 cafés mais vendidos:")
                    for coffee, count in coffee_result:
                        logger.info(f"     {coffee}: {count:,} vendas")
            
            logger.info("✅ Verificação concluída com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na verificação: {str(e)}")
            return False


def main():
    """Função principal para executar o processo de persistência"""
    logger.info("=" * 70)
    logger.info("🚀 TECH CHALLENGE 03 - PERSISTÊNCIA COFFEE SALES")
    logger.info("=" * 70)
    
    try:
        # Criar instância da classe de persistência
        persistencia = CoffeeSalesPersistencia()
        
        # 1. Testar conexão
        if not persistencia.test_connection():
            logger.error("❌ Falha na conexão com PostgreSQL")
            return 1
        
        # 2. Carregar dados do CSV
        df_original = persistencia.load_csv_data()
        
        # 3. Preparar dados
        df_clean = persistencia.prepare_data(df_original)
        
        # 4. Criar tabela
        if not persistencia.create_table(df_clean):
            logger.error("❌ Falha na criação da tabela")
            return 1
        
        # 5. Inserir dados
        if not persistencia.insert_data(df_clean):
            logger.error("❌ Falha na inserção dos dados")
            return 1
        
        # 6. Verificar dados inseridos
        if not persistencia.verify_data():
            logger.error("❌ Falha na verificação dos dados")
            return 1
        
        logger.info("=" * 70)
        logger.info("✅ PROCESSO DE PERSISTÊNCIA CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 70)
        logger.info("💡 Para consultar os dados:")
        logger.info(f"   docker-compose exec postgres psql -U admin -d techchallenge03")
        logger.info(f"   SELECT * FROM coffee_sales LIMIT 10;")
        
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