from abc import ABC, abstractmethod
from typing import List, Dict, Any, Callable
import pandas as pd
from tqdm import tqdm
import logging
from ...core.config import Config
from ...core.exceptions import IngestionError

logger = logging.getLogger(__name__)

class BaseProcessor(ABC):
    """Base class for data processors"""
    
    def __init__(self, config: Config):
        self.config = config
        self.batch_size = config.get('ingestion.batch_size', 100)
    
    def process_csv_in_batches(self, file_path: str, process_func: Callable[[pd.DataFrame], None]) -> None:
        """Process a CSV file in batches"""
        try:
            df = pd.read_csv(file_path)
            total_rows = len(df)
            
            with tqdm(total=total_rows, desc=f"Processing {file_path}", unit="rows",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                for start_idx in range(0, total_rows, self.batch_size):
                    end_idx = min(start_idx + self.batch_size, total_rows)
                    batch = df.iloc[start_idx:end_idx]
                    process_func(batch)
                    pbar.update(len(batch))
        except Exception as e:
            raise IngestionError(f"Failed to process CSV file {file_path}: {str(e)}")
    
    @abstractmethod
    def process(self, data: pd.DataFrame) -> None:
        """Process a batch of data"""
        pass
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate input data"""
        pass
    
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Transform data for ingestion"""
        pass
    
    @abstractmethod
    def ingest(self, data: List[Dict[str, Any]]) -> None:
        """Ingest transformed data"""
        pass 