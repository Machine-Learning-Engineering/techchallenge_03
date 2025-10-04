#!/usr/bin/env python3
"""
Data Upload Module for Tech Challenge 03
Download and process Spotify recommendation dataset from Kaggle
"""

import sys
import logging
import shutil
from pathlib import Path
from typing import Optional, Dict, Any
import kagglehub
from kagglehub import KaggleDatasetAdapter

# Configuração de logging - apenas terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SpotifyDataDownloader:
    """Classe para gerenciar o download e carregamento do dataset do Spotify"""
    
    def __init__(self, dataset_name: str = 'minahilfatima12328/daily-coffee-transactions'):
        """
        Inicializa o downloader
        
        Args:
            dataset_name: Nome do dataset no Kaggle
        """
        self.dataset_name = dataset_name
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
    def download_dataset(self, force_download: bool = False, copy_to_local: bool = True) -> str:
        """
        Baixa o dataset do Kaggle e opcionalmente copia para diretório local
        
        Args:
            force_download: Força o download mesmo se já existir
            copy_to_local: Se True, copia arquivos para diretório local do projeto
            
        Returns:
            str: Caminho onde os dados foram salvos (local se copy_to_local=True)
        """
        try:
            logger.info(f"Iniciando download do dataset: {self.dataset_name}")
            
            # Usar o método dataset_download para baixar os arquivos
            kaggle_cache_path = kagglehub.dataset_download(
                self.dataset_name, 
                force_download=force_download
            )
            
            logger.info(f"Dataset baixado do Kaggle em: {kaggle_cache_path}")
            
            if copy_to_local:
                local_path = self.copy_to_local_directory(kaggle_cache_path)
                logger.info(f"Arquivos copiados para diretório local: {local_path}")
                return local_path
            else:
                return kaggle_cache_path
            
        except Exception as e:
            logger.error(f"Erro ao baixar dataset: {str(e)}")
            raise
            
    def copy_to_local_directory(self, source_path: str) -> str:
        """
        Copia arquivos do cache do Kaggle para o diretório local do projeto
        
        Args:
            source_path: Caminho do cache do Kaggle
            
        Returns:
            str: Caminho do diretório local onde os arquivos foram copiados
        """
        try:
            source_dir = Path(source_path)
            local_data_dir = self.data_dir / 'coffee_dataset'
            
            # Criar diretório local se não existir
            local_data_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Copiando arquivos de {source_dir} para {local_data_dir}")
            
            copied_files = []
            for file_path in source_dir.glob('*'):
                if file_path.is_file():
                    destination = local_data_dir / file_path.name
                    shutil.copy2(file_path, destination)
                    copied_files.append(file_path.name)
                    logger.info(f"  ✅ Copiado: {file_path.name} ({file_path.stat().st_size} bytes)")
            
            logger.info(f"Total de {len(copied_files)} arquivo(s) copiado(s) para o diretório local")
            return str(local_data_dir)
            
        except Exception as e:
            logger.error(f"Erro ao copiar arquivos para diretório local: {str(e)}")
            raise
            
    def load_dataset_as_dataframe(self, file_path: str = "") -> Any:
        """
        Carrega o dataset como DataFrame usando kagglehub.load_dataset
        
        Args:
            file_path: Caminho específico do arquivo (opcional)
            
        Returns:
            DataFrame: Dataset carregado como pandas DataFrame
        """
        try:
            logger.info(f"Carregando dataset como DataFrame: {self.dataset_name}")
            
            # Verificar se pandas está disponível
            try:
                import pandas as pd
                logger.info("Pandas disponível para carregamento")
            except ImportError:
                logger.error("Pandas não está instalado")
                raise ImportError("Pandas é necessário para carregar como DataFrame")
            
            # Carregar usando KaggleDatasetAdapter.PANDAS
            df = kagglehub.load_dataset(
                KaggleDatasetAdapter.PANDAS,
                self.dataset_name,
                file_path,
                # Parâmetros adicionais do pandas podem ser passados aqui
                pandas_kwargs={"index_col": 0} if file_path else {}
            )
            
            logger.info(f"Dataset carregado com sucesso. Shape: {df.shape}")
            return df
            
        except ImportError as ie:
            logger.error(f"Erro de importação: {str(ie)}")
            logger.info("Instale com: pip install kagglehub[pandas-datasets]")
            raise
        except Exception as e:
            logger.error(f"Erro ao carregar dataset como DataFrame: {str(e)}")
            raise
            
    def verify_download(self, path: str) -> bool:
        """
        Verifica se o download foi bem-sucedido
        
        Args:
            path: Caminho dos dados baixados
            
        Returns:
            bool: True se o download foi bem-sucedido
        """
        try:
            download_path = Path(path)
            if not download_path.exists():
                logger.error(f"Caminho não existe: {path}")
                return False
                
            # Verificar se há arquivos no diretório
            files = list(download_path.glob('*'))
            if not files:
                logger.error(f"Nenhum arquivo encontrado em: {path}")
                return False
                
            logger.info(f"Verificação bem-sucedida. Arquivos encontrados:")
            for file in files:
                if file.is_file():
                    logger.info(f"  - {file.name} ({file.stat().st_size} bytes)")
                
            return True
            
        except Exception as e:
            logger.error(f"Erro na verificação: {str(e)}")
            return False
            
    def list_downloaded_files(self, path: str) -> list:
        """
        Lista os arquivos baixados
        
        Args:
            path: Caminho dos dados baixados
            
        Returns:
            list: Lista de arquivos encontrados
        """
        try:
            download_path = Path(path)
            if not download_path.exists():
                return []
                
            files = [str(file) for file in download_path.glob('*') if file.is_file()]
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Erro ao listar arquivos: {str(e)}")
            return []
            
    def get_local_csv_path(self) -> Optional[Path]:
        """
        Retorna o caminho do arquivo CSV local se existir
        
        Returns:
            Path: Caminho do arquivo CSV local ou None se não existir
        """
        local_data_dir = self.data_dir / 'spotify_dataset'
        csv_file = local_data_dir / 'data.csv'
        
        if csv_file.exists():
            return csv_file
        return None
        
    def load_local_csv_as_dataframe(self) -> Any:
        """
        Carrega o arquivo CSV local como DataFrame
        
        Returns:
            DataFrame: Dataset carregado como pandas DataFrame
        """
        try:
            import pandas as pd
            
            csv_path = self.get_local_csv_path()
            if not csv_path:
                raise FileNotFoundError("Arquivo CSV local não encontrado. Execute o download primeiro.")
            
            logger.info(f"Carregando CSV local: {csv_path}")
            df = pd.read_csv(csv_path, index_col=0)
            logger.info(f"CSV local carregado com sucesso. Shape: {df.shape}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao carregar CSV local: {str(e)}")
            raise


def setup_environment() -> bool:
    """Configura o ambiente necessário"""
    try:
        # Verificar se as credenciais do Kaggle estão configuradas
        kaggle_config = Path.home() / '.kaggle' / 'kaggle.json'
        if not kaggle_config.exists():
            logger.warning("Arquivo de credenciais do Kaggle não encontrado!")
            logger.info("Configure suas credenciais em: ~/.kaggle/kaggle.json")
            logger.info("Ou use as variáveis de ambiente KAGGLE_USERNAME e KAGGLE_KEY")
            
        # Verificar dependências
        try:
            import pandas as pd
            logger.info("✅ Pandas disponível")
        except ImportError:
            logger.warning("❌ Pandas não está disponível")
            logger.info("Instale com: pip install pandas")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Erro na configuração do ambiente: {str(e)}")
        return False


def main() -> int:
    """Função principal para download dos dados do Spotify"""
    logger.info("=" * 60)
    logger.info("TECH CHALLENGE 03 - SPOTIFY DATA UPLOAD")
    logger.info("=" * 60)
    
    try:
        # Configurar ambiente
        if not setup_environment():
            logger.error("Falha na configuração do ambiente")
            return 1
            
        # Criar instância do downloader
        downloader = SpotifyDataDownloader()
        
        # Método 1: Download e cópia para diretório local
        logger.info("📁 Método 1: Download e cópia para diretório local...")
        path_download = downloader.download_dataset(copy_to_local=True)
        
        if downloader.verify_download(path_download):
            files = downloader.list_downloaded_files(path_download)
            logger.info(f"✅ Download e cópia local concluídos. {len(files)} arquivo(s) salvos")
            
            # Mostrar alguns arquivos encontrados
            for i, file in enumerate(files[:5]):  # Mostrar até 5 arquivos
                logger.info(f"   📄 {Path(file).name}")
                
            # Verificar se CSV local está disponível
            csv_path = downloader.get_local_csv_path()
            if csv_path:
                logger.info(f"   🎯 Arquivo CSV principal: {csv_path}")
        else:
            logger.error("❌ Falha na verificação do download")
            return 1
            
        # Método 2: Carregamento do CSV local como DataFrame
        logger.info("📊 Método 2: Carregamento do CSV local como DataFrame...")
        try:
            df = downloader.load_local_csv_as_dataframe()
            logger.info(f"✅ CSV local carregado como DataFrame!")
            logger.info(f"   📏 Dimensões: {df.shape}")
            logger.info(f"   📋 Colunas: {list(df.columns)}")
            
            # Mostrar informações estatísticas básicas
            logger.info("📈 Informações estatísticas:")
            logger.info(f"   🎵 Total de músicas: {len(df)}")
            if 'liked' in df.columns:
                liked_count = df['liked'].sum()
                logger.info(f"   ❤️  Músicas curtidas: {liked_count}")
                logger.info(f"   💔 Músicas não curtidas: {len(df) - liked_count}")
            
            # Mostrar primeiras linhas
            logger.info("📖 Primeiras 5 linhas do dataset:")
            print("\n" + "="*70)
            print(df.head())
            print("="*70 + "\n")
            
        except Exception as e:
            logger.warning(f"⚠️  Erro ao carregar CSV local: {str(e)}")
            
            # Fallback: tentar método original do kagglehub
            try:
                logger.info("� Tentando método alternativo com kagglehub...")
                df = downloader.load_dataset_as_dataframe("data.csv")
                logger.info(f"✅ Dataset carregado via kagglehub!")
                logger.info(f"   📏 Dimensões: {df.shape}")
                
            except Exception as e2:
                logger.warning(f"⚠️  Método alternativo também falhou: {str(e2)}")
            
        logger.info("=" * 60)
        logger.info("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 60)
        
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