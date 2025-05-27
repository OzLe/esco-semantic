from typing import List, Dict, Any, Optional, Iterator
import pandas as pd
from tqdm import tqdm
from .translator import TranslationEngine
from ..core.logging import setup_logging

logger = setup_logging(__name__)

class BatchTranslator:
    """Handles efficient batch translation processing"""
    
    def __init__(
        self,
        translator: TranslationEngine,
        batch_size: int = 32,
        show_progress: bool = True
    ):
        self.translator = translator
        self.batch_size = batch_size
        self.show_progress = show_progress
    
    def translate_dataframe(
        self,
        df: pd.DataFrame,
        columns: List[str],
        output_columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Translate specified columns in a DataFrame"""
        if output_columns is None:
            output_columns = [f"{col}_translated" for col in columns]
        
        if len(columns) != len(output_columns):
            raise ValueError("Number of input and output columns must match")
        
        # Create copy of DataFrame
        result = df.copy()
        
        # Process each column
        for col, out_col in zip(columns, output_columns):
            # Get texts to translate
            texts = df[col].fillna('').tolist()
            
            # Translate in batches
            translations = self.translator.translate_batch(
                texts,
                batch_size=self.batch_size,
                show_progress=self.show_progress
            )
            
            # Add translations to result
            result[out_col] = translations
        
        return result
    
    def translate_file(
        self,
        input_file: str,
        output_file: str,
        columns: List[str],
        output_columns: Optional[List[str]] = None,
        file_format: str = 'csv'
    ) -> None:
        """Translate columns in a file and save results"""
        # Read input file
        if file_format == 'csv':
            df = pd.read_csv(input_file)
        elif file_format == 'excel':
            df = pd.read_excel(input_file)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Translate
        result = self.translate_dataframe(df, columns, output_columns)
        
        # Save results
        if file_format == 'csv':
            result.to_csv(output_file, index=False)
        else:
            result.to_excel(output_file, index=False)
    
    def translate_stream(
        self,
        texts: Iterator[str],
        batch_size: Optional[int] = None
    ) -> Iterator[str]:
        """Translate a stream of texts"""
        batch = []
        batch_size = batch_size or self.batch_size
        
        for text in texts:
            batch.append(text)
            
            if len(batch) >= batch_size:
                # Translate batch
                translations = self.translator.translate_batch(
                    batch,
                    batch_size=batch_size,
                    show_progress=False
                )
                
                # Yield translations
                for translation in translations:
                    yield translation
                
                # Clear batch
                batch = []
        
        # Process remaining texts
        if batch:
            translations = self.translator.translate_batch(
                batch,
                batch_size=batch_size,
                show_progress=False
            )
            for translation in translations:
                yield translation 