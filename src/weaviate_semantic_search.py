import logging
from typing import List, Dict, Optional, Any, Tuple
from sentence_transformers import SentenceTransformer
from src.logging_config import setup_logging
from src.weaviate_client import WeaviateClient
import torch
import os
import yaml

# Setup logging
logger = setup_logging()

def get_device() -> str:
    """Get the best available device for PyTorch."""
    if torch.cuda.is_available():
        return "cuda"
    elif torch.backends.mps.is_available():
        return "mps"
    return "cpu"

class ESCOSemanticSearch:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize the semantic search with configuration."""
        self.client = WeaviateClient(config_path, profile)
        self.model = SentenceTransformer('all-MiniLM-L6-v2', device=get_device())
        
        # Initialize repositories
        self.skill_repo = self.client.get_repository("Skill")
        self.occupation_repo = self.client.get_repository("Occupation")
        self.isco_group_repo = self.client.get_repository("ISCOGroup")
        self.skill_collection_repo = self.client.get_repository("SkillCollection")
        self.skill_group_repo = self.client.get_repository("SkillGroup")

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

    def _execute_query(self, query_string: str, node_type_for_error_logging: str) -> Optional[List[Dict]]:
        """Executes a GraphQL query and returns the results, handling basic errors."""
        try:
            logger.info(f"Executing {node_type_for_error_logging} query...")
            # Log the actual query for debugging
            logger.info(f"GraphQL query: {query_string[:500]}...")  # First 500 chars
            
            # Get the appropriate repository
            repo = self.client.get_repository(node_type_for_error_logging)
            
            # Execute the query using the repository
            result = repo._execute_raw_query(query_string)
            
            logger.info(f"Raw query result keys: {result.keys() if result else 'None'}")
            
            if not result or "data" not in result or "Get" not in result["data"] or node_type_for_error_logging not in result["data"]["Get"]:
                logger.error(f"Invalid Weaviate response for {node_type_for_error_logging} search: {result}")
                return None # Indicate error or empty result to caller
            
            query_results = result["data"]["Get"][node_type_for_error_logging]
            logger.info(f"Query returned {len(query_results)} {node_type_for_error_logging} results")
            
            return query_results
        except AttributeError as e:
            # This block catches AttributeErrors if parts of the client.query.raw chain are missing.
            logger.error(f"Error executing Weaviate query for {node_type_for_error_logging}: {e}. This might indicate an issue with the Weaviate client structure.")
            raise # Re-raise the exception to be handled by the caller or to stop execution
        except Exception as e:
            logger.error(f"Unexpected error during Weaviate query for {node_type_for_error_logging}: {str(e)}")
            return None # Indicate error to caller

    def get_related_graph(self, uri: str, node_type: str = "Skill") -> Optional[Dict]:
        """Get related graph for a node"""
        try:
            # Get the appropriate repository
            repo = self.client.get_repository(node_type)
            
            # Get the node
            node = repo.get_by_uri(uri)
            if not node:
                logger.warning(f"Node {uri} not found")
                return None
            
            # Get related skills using semantic search
            related_skills = self.skill_repo.search(
                query_vector=self.model.encode(node["preferredLabel_en"]),
                limit=10,
                certainty=0.7
            )
            
            # Format the result
            graph_data = {
                "node": {
                    "uri": node["conceptUri"],
                    "label": node["preferredLabel_en"],
                    "description": node.get("description_en", "")
                },
                "related_skills": [
                    {
                        "uri": skill["conceptUri"],
                        "label": skill["preferredLabel_en"],
                        "description": skill.get("description_en", ""),
                        "certainty": skill.get("_additional", {}).get("certainty", 0)
                    }
                    for skill in related_skills
                ]
            }
            
            return graph_data
            
        except Exception as e:
            logger.error(f"Error getting related graph for {uri}: {str(e)}")
            return None

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
        
        query_embedding_list = query_embedding.tolist()
        logger.debug(f"Generated embedding with {len(query_embedding_list)} dimensions for query: '{query_text}'")
        
        try:
            # First try with a very simple query to ensure basic connectivity works
            simple_test_query = f"""
            {{
                Get {{
                    {node_type}(limit: 1) {{
                        conceptUri
                        preferredLabel_en
                    }}
                }}
            }}
            """
            logger.info("Testing basic connectivity with simple query...")
            test_result = self._execute_query(simple_test_query, node_type)
            if test_result is None:
                logger.error("Basic connectivity test failed")
                return []
            logger.info(f"Basic connectivity test passed, found {len(test_result)} items")
            
            if node_type == "Skill":
                query = f"""
                {{
                    Get {{
                        Skill(
                            limit: {limit}
                            nearVector: {{
                                vector: {query_embedding_list}
                                certainty: {similarity_threshold}
                            }}
                        ) {{
                            conceptUri
                            preferredLabel_en
                            description_en
                            skillType
                            broaderSkill {{
                                ... on Skill {{
                                    conceptUri
                                    preferredLabel_en
                                }}
                            }}
                            memberOfSkillCollection {{
                                ... on SkillCollection {{
                                    conceptUri
                                    preferredLabel_en
                                }}
                            }}
                            hasRelatedSkill {{
                                ... on Skill {{
                                    conceptUri
                                    preferredLabel_en
                                }}
                            }}
                            _additional {{
                                certainty
                            }}
                        }}
                    }}
                }}
                """
                logger.debug(f"Executing Skill query with similarity threshold {similarity_threshold}")
                result = self._execute_query(query, "Skill")
                if not result:
                    logger.warning(f"No results returned from Skill query with threshold {similarity_threshold}, trying with lower threshold")
                    # Try with a much lower threshold as fallback
                    fallback_query = query.replace(str(similarity_threshold), "0.1")
                    result = self._execute_query(fallback_query, "Skill")
                    if not result:
                        logger.warning("No results even with fallback threshold")
                        return []
                logger.debug(f"Found {len(result)} skill results")
                results = result
                
            elif node_type == "Occupation":
                query = f"""
                {{
                    Get {{
                        Occupation(
                            limit: {limit}
                            nearVector: {{
                                vector: {query_embedding_list}
                                certainty: {similarity_threshold}
                            }}
                        ) {{
                            conceptUri
                            preferredLabel_en
                            description_en
                            code
                            broaderOccupation {{
                                ... on Occupation {{
                                    conceptUri
                                    preferredLabel_en
                                    code
                                }}
                            }}
                            hasEssentialSkill {{
                                ... on Skill {{
                                    conceptUri
                                    preferredLabel_en
                                    skillType
                                }}
                            }}
                            hasOptionalSkill {{
                                ... on Skill {{
                                    conceptUri
                                    preferredLabel_en
                                    skillType
                                }}
                            }}
                            _additional {{
                                certainty
                            }}
                        }}
                    }}
                }}
                """
                logger.debug(f"Executing Occupation query with similarity threshold {similarity_threshold}")
                result = self._execute_query(query, "Occupation")
                if not result:
                    logger.warning(f"No results returned from Occupation query with threshold {similarity_threshold}, trying with lower threshold")
                    # Try with a much lower threshold as fallback
                    fallback_query = query.replace(str(similarity_threshold), "0.1")
                    result = self._execute_query(fallback_query, "Occupation")
                    if not result:
                        logger.warning("No results even with fallback threshold")
                        return []
                logger.debug(f"Found {len(result)} occupation results")
                results = result
                
            else:
                # Search both skills and occupations
                skill_query = f"""
                {{
                    Get {{
                        Skill(
                            limit: {limit}
                            nearVector: {{
                                vector: {query_embedding_list}
                                certainty: {similarity_threshold}
                            }}
                        ) {{
                            conceptUri
                            preferredLabel_en
                            description_en
                            skillType
                            _additional {{
                                certainty
                            }}
                        }}
                    }}
                }}
                """
                
                occupation_query = f"""
                {{
                    Get {{
                        Occupation(
                            limit: {limit}
                            nearVector: {{
                                vector: {query_embedding_list}
                                certainty: {similarity_threshold}
                            }}
                        ) {{
                            conceptUri
                            preferredLabel_en
                            description_en
                            code
                            _additional {{
                                certainty
                            }}
                        }}
                    }}
                }}
                """
                
                skill_result = self._execute_query(skill_query, "Skill")
                occupation_result = self._execute_query(occupation_query, "Occupation")
                
                # Handle cases where a query might fail or return no results
                skills = skill_result if skill_result is not None else []
                occupations = occupation_result if occupation_result is not None else []

                if not skills and not occupations:
                    logger.info("No results found for either Skills or Occupations in combined search.")
                    return []
                
                # Combine and sort results
                results = []
                for item in skills:
                    item["type"] = "Skill"
                    results.append(item)
                for item in occupations:
                    item["type"] = "Occupation"
                    results.append(item)
                
                # Sort by certainty
                results.sort(key=lambda x: x["_additional"]["certainty"], reverse=True)
                results = results[:limit]
            
            # Process results
            return results
            
        except Exception as e:
            logger.error(f"Error during semantic search: {str(e)}")
            return []

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
                if profile["related_skills"]:
                    output.append("\n   Essential Skills:")
                    for skill in profile["related_skills"]:
                        output.append(f"   • {skill['label']}")
            else:
                # Handle basic search results
                output.append(f"{i}. [{result.get('type', 'Result')}] {result['label']}")
                output.append(f"   URI: {result['uri']}")
                output.append(f"   Similarity: {result['score']:.2%}")
                if result.get("description"):
                    output.append(f"   Description: {result['description']}")
            
            output.append("\n" + "-" * 72)
        
        return "\n".join(output) 