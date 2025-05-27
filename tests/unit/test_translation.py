import pytest
from unittest.mock import Mock, patch
import pandas as pd
from esco.translation.translator import TranslationEngine
from esco.translation.batch import BatchTranslator
from esco.translation.models import TranslationModelManager

class TestTranslationEngine:
    @pytest.fixture
    def mock_model(self):
        model = Mock()
        model.generate.return_value = [[1, 2, 3]]  # Mock token IDs
        return model
    
    @pytest.fixture
    def mock_tokenizer(self):
        tokenizer = Mock()
        tokenizer.decode.return_value = "Translated text"
        return tokenizer
    
    @pytest.fixture
    def mock_model_manager(self, mock_model, mock_tokenizer):
        manager = Mock()
        manager.get_model.return_value = (mock_model, mock_tokenizer)
        return manager
    
    def test_translate(self, mock_model_manager):
        with patch('esco.translation.translator.TranslationModelManager',
                  return_value=mock_model_manager):
            engine = TranslationEngine()
            result = engine.translate("Test text")
            
            assert result == "Translated text"
    
    def test_translate_batch(self, mock_model_manager):
        with patch('esco.translation.translator.TranslationModelManager',
                  return_value=mock_model_manager):
            engine = TranslationEngine()
            texts = ["Text 1", "Text 2", "Text 3"]
            results = engine.translate_batch(texts)
            
            assert len(results) == 3
            assert all(r == "Translated text" for r in results)
    
    def test_get_supported_languages(self, mock_model_manager):
        with patch('esco.translation.translator.TranslationModelManager',
                  return_value=mock_model_manager):
            engine = TranslationEngine()
            languages = engine.get_supported_languages()
            
            assert isinstance(languages, dict)
            mock_model_manager.get_supported_languages.assert_called_once()

class TestBatchTranslator:
    @pytest.fixture
    def mock_translator(self):
        translator = Mock()
        translator.translate_batch.return_value = [
            "Translated 1",
            "Translated 2",
            "Translated 3"
        ]
        return translator
    
    def test_translate_dataframe(self, mock_translator):
        # Create test DataFrame
        df = pd.DataFrame({
            'text': ['Text 1', 'Text 2', 'Text 3'],
            'other': ['Other 1', 'Other 2', 'Other 3']
        })
        
        translator = BatchTranslator(mock_translator)
        result = translator.translate_dataframe(
            df=df,
            columns=['text'],
            output_columns=['translated']
        )
        
        assert 'translated' in result.columns
        assert result['translated'].tolist() == [
            "Translated 1",
            "Translated 2",
            "Translated 3"
        ]
    
    def test_translate_stream(self, mock_translator):
        texts = ["Text 1", "Text 2", "Text 3"]
        translator = BatchTranslator(mock_translator)
        
        results = list(translator.translate_stream(texts))
        
        assert results == [
            "Translated 1",
            "Translated 2",
            "Translated 3"
        ]

class TestTranslationModelManager:
    def test_get_supported_languages(self):
        manager = TranslationModelManager()
        languages = manager.get_supported_languages()
        
        assert isinstance(languages, dict)
        assert 'en' in languages
        assert 'es' in languages
    
    def test_is_supported(self):
        manager = TranslationModelManager()
        
        assert manager.is_supported('en', 'es')
        assert manager.is_supported('es', 'en')
        assert not manager.is_supported('en', 'xx')  # Non-existent language
    
    @patch('transformers.MarianMTModel.from_pretrained')
    @patch('transformers.MarianTokenizer.from_pretrained')
    def test_get_model(self, mock_tokenizer, mock_model):
        manager = TranslationModelManager()
        model, tokenizer = manager.get_model('test-model')
        
        assert model == mock_model.return_value
        assert tokenizer == mock_tokenizer.return_value
        mock_model.assert_called_once_with('test-model')
        mock_tokenizer.assert_called_once_with('test-model') 