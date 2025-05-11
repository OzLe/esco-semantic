from sentence_transformers import SentenceTransformer
import logging
from tqdm import tqdm

class ESCOEmbedding:
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        """Initialize with a sentence transformer model"""
        self.model = SentenceTransformer(model_name)
        self.vector_dim = self.model.get_sentence_embedding_dimension()
        logging.info(f"Initialized embedding model: {model_name} (dim: {self.vector_dim})")
        
    def generate_text_embedding(self, text):
        """Generate embedding for a single text"""
        if not text:
            return None
        return self.model.encode(text).tolist()
    
    def generate_node_embedding(self, node_data):
        """Generate embedding from node data combining label and description"""
        label = node_data.get('preferredLabel', '')
        desc = node_data.get('description', '')
        alt_labels = node_data.get('altLabels', '')
        
        # Combine text fields for richer embedding
        text = f"{label}. {alt_labels}. {desc}".strip()
        if not text:
            return None
            
        return self.generate_text_embedding(text)