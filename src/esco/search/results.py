from typing import List, Dict, Any, Optional
from .engine import SearchResult
from ..core.logging import setup_logging

logger = setup_logging(__name__)

class SearchResultFormatter:
    """Formats search results for different output formats"""
    
    @staticmethod
    def to_dict(results: List[SearchResult]) -> List[Dict[str, Any]]:
        """Convert results to list of dictionaries"""
        return [result.to_dict() for result in results]
    
    @staticmethod
    def to_json(results: List[SearchResult], pretty: bool = False) -> str:
        """Convert results to JSON string"""
        import json
        data = [result.to_dict() for result in results]
        if pretty:
            return json.dumps(data, indent=2)
        return json.dumps(data)
    
    @staticmethod
    def to_text(results: List[SearchResult], include_score: bool = True) -> str:
        """Convert results to formatted text"""
        lines = []
        for i, result in enumerate(results, 1):
            line = f"{i}. {result.label}"
            if result.description:
                line += f"\n   {result.description}"
            if include_score:
                line += f"\n   Score: {result.score:.3f}"
            if result.type != "unknown":
                line += f"\n   Type: {result.type}"
            lines.append(line)
        return "\n\n".join(lines)
    
    @staticmethod
    def to_markdown(results: List[SearchResult], include_score: bool = True) -> str:
        """Convert results to markdown format"""
        lines = []
        for i, result in enumerate(results, 1):
            lines.append(f"### {i}. {result.label}")
            if result.description:
                lines.append(f"\n{result.description}\n")
            if include_score:
                lines.append(f"**Score:** {result.score:.3f}")
            if result.type != "unknown":
                lines.append(f"**Type:** {result.type}")
            lines.append("---\n")
        return "\n".join(lines)
    
    @staticmethod
    def to_html(results: List[SearchResult], include_score: bool = True) -> str:
        """Convert results to HTML format"""
        lines = ['<div class="search-results">']
        for i, result in enumerate(results, 1):
            lines.append(f'<div class="result-item">')
            lines.append(f'<h3>{i}. {result.label}</h3>')
            if result.description:
                lines.append(f'<p>{result.description}</p>')
            if include_score:
                lines.append(f'<p><strong>Score:</strong> {result.score:.3f}</p>')
            if result.type != "unknown":
                lines.append(f'<p><strong>Type:</strong> {result.type}</p>')
            lines.append('</div>')
        lines.append('</div>')
        return "\n".join(lines) 