import logging
from embedding_utils import ESCOEmbedding
from logging_config import setup_logging
from weaviate_client import WeaviateClient
from typing import List, Dict, Optional, Any, Tuple

# Setup logging
logger = setup_logging()

class ESCOSemanticSearch:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default", embedding_util=None):
        """Initialize with Weaviate client and optional embedding utility"""
        self.client = WeaviateClient(config_path, profile)
        self.embedding_util = embedding_util or ESCOEmbedding()
    
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
            - is_valid: True if all required data is present and valid
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
            
            # Validate relationships if both types are indexed
            if validation_details["skills_indexed"] and validation_details["occupations_indexed"]:
                # Sample check for relationships
                sample_skill = self.client.client.query.get("Skill", ["conceptUri"]).with_limit(1).do()
                if sample_skill["data"]["Get"]["Skill"]:
                    skill_uri = sample_skill["data"]["Get"]["Skill"][0]["conceptUri"]
                    related_occupations = self.client.client.query.get(
                        "Occupation", ["conceptUri"]
                    ).with_where({
                        "path": ["hasEssentialSkill"],
                        "operator": "ContainsAny",
                        "valueString": [skill_uri]
                    }).do()
                    validation_details["has_relationships"] = len(related_occupations["data"]["Get"]["Occupation"]) > 0
            
            is_valid = validation_details["skills_indexed"] and validation_details["occupations_indexed"]
            
            if not is_valid:
                validation_details["errors"].append("Missing required data")
            
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
        
        # Generate query embedding
        query_embedding = self.embedding_util.generate_text_embedding(query_text)
        if not query_embedding:
            logger.error("Failed to generate embedding for query text")
            return []
        
        try:
            if node_type == "Skill":
                result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel", "description"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                results = result["data"]["Get"]["Skill"]
            elif node_type == "Occupation":
                result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel", "description"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                results = result["data"]["Get"]["Occupation"]
            else:
                # Search both skills and occupations
                skill_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel", "description"])
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
                    .get("Occupation", ["conceptUri", "preferredLabel", "description"])
                    .with_near_vector({
                        "vector": query_embedding,
                        "certainty": similarity_threshold
                    })
                    .with_limit(limit)
                    .with_additional(["certainty"])
                    .do()
                )
                
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
                    "label": record["preferredLabel"],
                    "description": record.get("description", ""),
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
                    .get("Skill", ["conceptUri", "preferredLabel", "description"])
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
                    .get("Occupation", ["conceptUri", "preferredLabel", "description"])
                    .with_where({
                        "path": ["hasEssentialSkill"],
                        "operator": "ContainsAny",
                        "valueString": [uri]
                    })
                    .do()
                )
                
                optional_occupation_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel", "description"])
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
                    .get("Skill", ["conceptUri", "preferredLabel", "description"])
                    .with_near_vector({
                        "vector": self.embedding_util.generate_text_embedding(skill["preferredLabel"]),
                        "certainty": 0.7
                    })
                    .with_limit(10)
                    .do()
                )
                
                # Format the result
                graph_data = {
                    "node": {
                        "uri": skill["conceptUri"],
                        "label": skill["preferredLabel"],
                        "description": skill.get("description", "")
                    },
                    "related": {
                        "essential_occupations": [
                            {
                                "uri": o["conceptUri"],
                                "label": o["preferredLabel"],
                                "description": o.get("description", "")
                            }
                            for o in occupation_result["data"]["Get"]["Occupation"]
                        ],
                        "optional_occupations": [
                            {
                                "uri": o["conceptUri"],
                                "label": o["preferredLabel"],
                                "description": o.get("description", "")
                            }
                            for o in optional_occupation_result["data"]["Get"]["Occupation"]
                        ],
                        "related_skills": [
                            {
                                "uri": s["conceptUri"],
                                "label": s["preferredLabel"],
                                "description": s.get("description", "")
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
                    .get("Occupation", ["conceptUri", "preferredLabel", "description"])
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
                    .get("Skill", ["conceptUri", "preferredLabel", "description"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "In",
                        "valueString": occupation.get("hasEssentialSkill", [])
                    })
                    .do()
                )
                
                # Get optional skills
                optional_skills_result = (
                    self.client.client.query
                    .get("Skill", ["conceptUri", "preferredLabel", "description"])
                    .with_where({
                        "path": ["conceptUri"],
                        "operator": "In",
                        "valueString": occupation.get("hasOptionalSkill", [])
                    })
                    .do()
                )
                
                # Get related occupations
                related_occupations_result = (
                    self.client.client.query
                    .get("Occupation", ["conceptUri", "preferredLabel", "description"])
                    .with_near_vector({
                        "vector": self.embedding_util.generate_text_embedding(occupation["preferredLabel"]),
                        "certainty": 0.7
                    })
                    .with_limit(10)
                    .do()
                )
                
                # Format the result
                graph_data = {
                    "node": {
                        "uri": occupation["conceptUri"],
                        "label": occupation["preferredLabel"],
                        "description": occupation.get("description", "")
                    },
                    "related": {
                        "essential_skills": [
                            {
                                "uri": s["conceptUri"],
                                "label": s["preferredLabel"],
                                "description": s.get("description", "")
                            }
                            for s in essential_skills_result["data"]["Get"]["Skill"]
                        ],
                        "optional_skills": [
                            {
                                "uri": s["conceptUri"],
                                "label": s["preferredLabel"],
                                "description": s.get("description", "")
                            }
                            for s in optional_skills_result["data"]["Get"]["Skill"]
                        ],
                        "related_occupations": [
                            {
                                "uri": o["conceptUri"],
                                "label": o["preferredLabel"],
                                "description": o.get("description", "")
                            }
                            for o in related_occupations_result["data"]["Get"]["Occupation"]
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