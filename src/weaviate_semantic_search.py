import logging
from typing import List, Dict, Optional, Any, Tuple
from sentence_transformers import SentenceTransformer
from src.logging_config import setup_logging
from src.weaviate_client import WeaviateClient
import torch
import os

# Setup logging
logger = setup_logging()

def get_device():
    """Get the best available device for PyTorch operations."""
    if torch.backends.mps.is_available():
        return "mps"
    elif torch.cuda.is_available():
        return "cuda"
    return "cpu"

class ESCOSemanticSearch:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default", 
                 embedding_model: str = 'sentence-transformers/multi-qa-MiniLM-L6-cos-v1'):
        """Initialize with Weaviate client and embedding model
        
        Args:
            config_path: Path to Weaviate configuration file
            profile: Configuration profile to use
            embedding_model: Name of the sentence transformer model to use
        """
        self.client = WeaviateClient(config_path, profile)
        
        # Get device from environment or auto-detect
        device = os.getenv('TORCH_DEVICE', get_device())
        if device == "mps" and not torch.backends.mps.is_available():
            logger.warning("MPS requested but not available, falling back to CPU")
            device = "cpu"
        
        logger.info(f"Using device: {device}")
        self.model = SentenceTransformer(embedding_model, device=device)
    
    def is_data_indexed(self, node_type="Skill") -> bool:
        """Check if the data is already indexed with embeddings"""
        try:
            if node_type == "Skill":
                result = self.client.client.query.aggregate("Skill").with_meta_count().do()
                return result["data"]["Aggregate"]["Skill"][0]["meta"]["count"] > 0
            elif node_type == "Occupation":
                result = self.client.client.query.aggregate("Occupation").with_meta_count().do()
                return result["data"]["Aggregate"]["Occupation"][0]["meta"]["count"] > 0
            else:
                skill_count = self.client.client.query.aggregate("Skill").with_meta_count().do()
                occupation_count = self.client.client.query.aggregate("Occupation").with_meta_count().do()
                return (skill_count["data"]["Aggregate"]["Skill"][0]["meta"]["count"] > 0 or 
                        occupation_count["data"]["Aggregate"]["Occupation"][0]["meta"]["count"] > 0)
        except Exception as e:
            logger.error(f"Error checking if data is indexed: {str(e)}")
            return False

    def validate_data(self) -> Tuple[bool, Dict[str, Any]]:
        """Validate the data in the Weaviate database
        
        Returns:
            Tuple[bool, Dict[str, Any]]: (is_valid, validation_details)
            - is_valid: True if at least one type of data is present and valid
            - validation_details: Dictionary containing detailed validation results
        """
        validation_details = {
            "skills_indexed": False,
            "occupations_indexed": False,
            "skills_count": 0,
            "occupations_count": 0,
            "errors": []
        }
        
        try:
            # Check Skills
            skill_result = self.client.client.query.aggregate("Skill").with_meta_count().do()
            validation_details["skills_count"] = skill_result["data"]["Aggregate"]["Skill"][0]["meta"]["count"]
            validation_details["skills_indexed"] = validation_details["skills_count"] > 0
            
            # Check Occupations
            occupation_result = self.client.client.query.aggregate("Occupation").with_meta_count().do()
            validation_details["occupations_count"] = occupation_result["data"]["Aggregate"]["Occupation"][0]["meta"]["count"]
            validation_details["occupations_indexed"] = validation_details["occupations_count"] > 0
            
            # Consider valid if at least one type is indexed
            is_valid = validation_details["skills_indexed"] or validation_details["occupations_indexed"]
            
            if not is_valid:
                validation_details["errors"].append("No data is indexed")
            
            return is_valid, validation_details
            
        except Exception as e:
            error_msg = f"Error during data validation: {str(e)}"
            logger.error(error_msg)
            validation_details["errors"].append(error_msg)
            return False, validation_details

    def search(self, query_text: str, node_type: str = "Skill", limit: int = 10, 
               search_only: bool = False, similarity_threshold: float = 0.5) -> List[Dict]:
        """Search for semantically similar nodes
        
        Args:
            query_text (str): The text to search for
            node_type (str): Type of nodes to search ("Skill", "Occupation", or "Both")
            limit (int): Maximum number of results to return
            search_only (bool): If True, only perform search without re-indexing
            similarity_threshold (float): Minimum similarity score (0.0 to 1.0)
        """
        # Validate data if in search-only mode
        if search_only:
            is_valid, validation_details = self.validate_data()
            if not is_valid:
                error_msg = "Data validation failed. Please run the full pipeline first or disable search-only mode."
                logger.error(f"{error_msg} Validation details: {validation_details}")
                raise ValueError(error_msg)
        
        # Generate query embedding using the sentence transformer model
        query_embedding = self.model.encode(query_text)
        if query_embedding is None or query_embedding.size == 0:
            logger.error("Failed to generate embedding for query text")
            return []
        
        try:
            if node_type == "Skill":
                result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                if not result or "data" not in result:
                    logger.error(f"Invalid Weaviate response for Skill search: {result}")
                    return []
                results = result["data"]["Get"]["Skill"]
            elif node_type == "Occupation":
                result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                if not result or "data" not in result:
                    logger.error(f"Invalid Weaviate response for Occupation search: {result}")
                    return []
                results = result["data"]["Get"]["Occupation"]
            else:
                # Search both skills and occupations
                skill_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                
                occupation_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                
                if not skill_result or "data" not in skill_result:
                    logger.error(f"Invalid Weaviate response for Skill search: {skill_result}")
                    skill_result = {"data": {"Get": {"Skill": []}}}
                
                if not occupation_result or "data" not in occupation_result:
                    logger.error(f"Invalid Weaviate response for Occupation search: {occupation_result}")
                    occupation_result = {"data": {"Get": {"Occupation": []}}}
                
                # Combine and sort results
                results = []
                for item in skill_result["data"]["Get"]["Skill"]:
                    item["type"] = "Skill"
                    results.append(item)
                for item in occupation_result["data"]["Get"]["Occupation"]:
                    item["type"] = "Occupation"
                    results.append(item)
                
                # Sort by certainty
                results.sort(key=lambda x: x["_additional"]["certainty"], reverse=True)
                results = results[:limit]
            
            # Process results
            search_results = []
            for record in results:
                search_results.append({
                    "uri": record["conceptUri"],
                    "label": record["preferredLabel_en"],
                    "description": record.get("description_en", ""),
                    "type": record.get("type", node_type),
                    "score": record["_additional"]["certainty"]
                })
            
            return search_results
            
        except Exception as e:
            logger.error(f"Error during semantic search: {str(e)}")
            return []

    def get_related_graph(self, uri: str, node_type: str = "Skill") -> Optional[Dict]:
        """Get related graph for a node"""
        try:
            if node_type == "Skill":
                # Get the skill node
                skill_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "Equal",
                        "valueString": uri
                    })
                    .do()
                )
                
                if not skill_result["data"]["Get"]["Skill"]:
                    return None
                
                skill = skill_result["data"]["Get"]["Skill"][0]
                
                # Get related occupations
                occupation_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_where({
                        "path": ["hasEssentialSkill"],
                        "operator": "ContainsAny",
                        "valueString": [uri]
                    })
                    .do()
                )
                
                optional_occupation_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_where({
                        "path": ["hasOptionalSkill"],
                        "operator": "ContainsAny",
                        "valueString": [uri]
                    })
                    .do()
                )
                
                # Get related skills
                related_skills_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_near_vector({
                        "vector": self.model.encode(skill["preferredLabel_en"]),
                        "certainty": 0.7
                    })
                    .with_limit(10)
                    .do()
                )
                
                # Format the result
                graph_data = {
                    "node": {
                        "uri": skill["conceptUri"],
                        "label": skill["preferredLabel_en"],
                        "description": skill.get("description_en", "")
                    },
                    "related": {
                        "essential_occupations": [
                            {
                                "uri": o["conceptUri"],
                                "label": o["preferredLabel_en"],
                                "description": o.get("description_en", "")
                            }
                            for o in occupation_result["data"]["Get"]["Occupation"]
                        ],
                        "optional_occupations": [
                            {
                                "uri": o["conceptUri"],
                                "label": o["preferredLabel_en"],
                                "description": o.get("description_en", "")
                            }
                            for o in optional_occupation_result["data"]["Get"]["Occupation"]
                        ],
                        "related_skills": [
                            {
                                "uri": s["conceptUri"],
                                "label": s["preferredLabel_en"],
                                "description": s.get("description_en", "")
                            }
                            for s in related_skills_result["data"]["Get"]["Skill"]
                            if s["conceptUri"] != uri  # Exclude the original skill
                        ]
                    }
                }
                
            else:  # Occupation
                # Get the occupation node
                occupation_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "Equal",
                        "valueString": uri
                    })
                    .do()
                )
                
                if not occupation_result["data"]["Get"]["Occupation"]:
                    return None
                
                occupation = occupation_result["data"]["Get"]["Occupation"][0]
                
                # Get essential skills
                essential_skills_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "ContainsAny",
                        "valueString": occupation.get("hasEssentialSkill", [])
                    })
                    .do()
                )
                
                # Get optional skills
                optional_skills_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "ContainsAny",
                        "valueString": occupation.get("hasOptionalSkill", [])
                    })
                    .do()
                )
                
                # Get related occupations
                related_occupations_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel_en", "description_en"])
                    .with_near_vector({
                        "vector": self.model.encode(occupation["preferredLabel_en"]),
                        "certainty": 0.7
                    })
                    .with_limit(10)
                    .do()
                )
                
                # Format the result
                graph_data = {
                    "node": {
                        "uri": occupation["conceptUri"],
                        "label": occupation["preferredLabel_en"],
                        "description": occupation.get("description_en", "")
                    },
                    "related": {
                        "essential_skills": [
                            {
                                "uri": s["conceptUri"],
                                "label": s["preferredLabel_en"],
                                "description": s.get("description_en", "")
                            }
                            for s in (essential_skills_result.get("data", {}).get("Get", {}).get("Skill", []) or [])
                        ],
                        "optional_skills": [
                            {
                                "uri": s["conceptUri"],
                                "label": s["preferredLabel_en"],
                                "description": s.get("description_en", "")
                            }
                            for s in (optional_skills_result.get("data", {}).get("Get", {}).get("Skill", []) or [])
                        ],
                        "related_occupations": [
                            {
                                "uri": o["conceptUri"],
                                "label": o["preferredLabel_en"],
                                "description": o.get("description_en", "")
                            }
                            for o in (related_occupations_result.get("data", {}).get("Get", {}).get("Occupation", []) or [])
                            if o["conceptUri"] != uri  # Exclude the original occupation
                        ]
                    }
                }
            
            return graph_data
            
        except Exception as e:
            logger.error(f"Error getting related graph: {str(e)}")
            return None

    def semantic_search_with_profile(self, query_text: str, limit: int = 10, 
                                   similarity_threshold: float = 0.5) -> List[Dict]:
        """Perform semantic search and retrieve complete occupation profiles
        
        Args:
            query_text (str): The text to search for
            limit (int): Maximum number of results to return
            similarity_threshold (float): Minimum similarity score (0.0 to 1.0)
            
        Returns:
            list: List of dictionaries containing search results with complete occupation profiles
        """
        # Validate data before proceeding
        is_valid, validation_details = self.validate_data()
        if not is_valid:
            error_msg = "Data validation failed. Please ensure all required data is indexed."
            logger.error(f"{error_msg} Validation details: {validation_details}")
            raise ValueError(error_msg)
            
        # First hop: Perform semantic search for occupations
        search_results = self.search(
            query_text=query_text,
            node_type="Occupation",
            limit=limit,
            similarity_threshold=similarity_threshold
        )
        
        # Second hop: Retrieve complete profiles for each result
        complete_results = []
        for result in search_results:
            profile = self.get_related_graph(result["uri"], node_type="Occupation")
            if profile:
                # Combine search result with complete profile
                complete_result = {
                    "search_result": result,
                    "profile": profile
                }
                complete_results.append(complete_result)
        
        return complete_results

    def format_results(self, results: List[Dict]) -> str:
        """Format search results as a human-readable string.
        
        Args:
            results: List of search results from search() method
            
        Returns:
            Formatted string representation of results
        """
        if not results:
            return "No results found."
        
        output = []
        output.append("=" * 72)
        output.append(" ESCO Semantic Search Results ")
        output.append("=" * 72)
        output.append("")
        
        for i, result in enumerate(results, 1):
            if isinstance(result, dict) and "search_result" in result:
                # Handle complete profile results
                occupation = result["search_result"]
                profile = result["profile"]
                
                # Format occupation
                output.append(f"{i}. [Occupation] {occupation['label']}")
                output.append(f"   URI: {occupation['uri']}")
                output.append(f"   Similarity: {occupation['score']:.2%}")
                if occupation.get("description"):
                    output.append(f"   Description: {occupation['description']}")
                
                # Format essential skills
                if profile["related"]["essential_skills"]:
                    output.append("\n   Essential Skills:")
                    for skill in profile["related"]["essential_skills"]:
                        output.append(f"   • {skill['label']}")
                
                # Format optional skills
                if profile["related"]["optional_skills"]:
                    output.append("\n   Optional Skills:")
                    for skill in profile["related"]["optional_skills"]:
                        output.append(f"   • {skill['label']}")
                
                # Format related occupations
                if profile["related"]["related_occupations"]:
                    output.append("\n   Related Occupations:")
                    for occ in profile["related"]["related_occupations"]:
                        output.append(f"   • {occ['label']}")
            else:
                # Handle basic search results
                output.append(f"{i}. [{result.get('type', 'Result')}] {result['label']}")
                output.append(f"   URI: {result['uri']}")
                output.append(f"   Similarity: {result['score']:.2%}")
                if result.get("description"):
                    output.append(f"   Description: {result['description']}")
            
            output.append("\n" + "-" * 72)
        
        return "\n".join(output) 