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

    def _build_skill_query_string(self, query_embedding_list: List[float], limit: int, similarity_threshold: float, skill_fragment: str) -> str:
        """Builds the GraphQL query string for Skill search."""
        return f"""
        {skill_fragment}
        {{
            Get {{
                Skill(
                    limit: {limit}
                    nearVector: {{
                        vector: {query_embedding_list}
                        certainty: {similarity_threshold}
                    }}
                ) {{
                    ...SkillFields
                    _additional {{
                        certainty
                    }}
                }}
            }}
        }}
        """

    def _build_occupation_query_string(self, query_embedding_list: List[float], limit: int, similarity_threshold: float, skill_fragment: str, occupation_fragment: str) -> str:
        """Builds the GraphQL query string for Occupation search."""
        return f"""
        {skill_fragment}
        {occupation_fragment}
        {{
            Get {{
                Occupation(
                    limit: {limit}
                    nearVector: {{
                        vector: {query_embedding_list}
                        certainty: {similarity_threshold}
                    }}
                ) {{
                    ...OccupationFields
                    _additional {{
                        certainty
                    }}
                }}
            }}
        }}
        """

    def _execute_query(self, query_string: str, node_type_for_error_logging: str) -> Optional[List[Dict]]:
        """Executes a GraphQL query and returns the results, handling basic errors."""
        try:
            logger.info(f"Executing {node_type_for_error_logging} query...")
            # Log the actual query for debugging
            logger.info(f"GraphQL query: {query_string[:500]}...")  # First 500 chars
            
            # client.query.raw() executes the GraphQL query string directly.
            result = self.client.client.query.raw(query_string)
            
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

    def _process_search_results(self, raw_results: List[Dict], node_type: str) -> List[Dict]:
        """Processes raw search results into the final format."""
        search_results = []
        for record in raw_results:
            result_dict = {
                "uri": record["conceptUri"],
                "label": record["preferredLabel_en"],
                "description": record.get("description_en", ""),
                "type": record.get("type", node_type), # Ensure 'type' is present
                "score": record["_additional"]["certainty"]
            }
            
            # Add additional fields based on node type
            current_record_type = record.get("type", node_type)
            if current_record_type == "Skill":
                result_dict.update({
                    "skillType": record.get("skillType"),
                    "broaderSkills": self._process_broader_skills(record.get("broaderSkill", [])),
                    "skillCollections": [
                        {"uri": c["conceptUri"], "label": c["preferredLabel_en"]}
                        for c in record.get("memberOfSkillCollection", [])
                    ],
                    "relatedSkills": [
                        {
                            "uri": s["conceptUri"],
                            "label": s["preferredLabel_en"]
                        }
                        for s in record.get("hasRelatedSkill", [])
                    ]
                })
            elif current_record_type == "Occupation":
                result_dict.update({
                    "code": record.get("code"),
                    "broaderOccupations": self._process_broader_occupations(record.get("broaderOccupation", [])),
                    "essentialSkills": self._process_skills(record.get("hasEssentialSkill", [])),
                    "optionalSkills": self._process_skills(record.get("hasOptionalSkill", []))
                })
            
            search_results.append(result_dict)
        return search_results

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
            return self._process_search_results(results, node_type)
            
        except Exception as e:
            logger.error(f"Error during semantic search: {str(e)}")
            return []

    def _process_broader_skills(self, broader_skills):
        """Helper method to process broader skills hierarchy"""
        if not broader_skills:
            return []
        
        result = []
        for skill in broader_skills:
            skill_dict = {
                "uri": skill["conceptUri"],
                "label": skill["preferredLabel_en"]
            }
            if "broaderSkill" in skill:
                skill_dict["broaderSkills"] = self._process_broader_skills(skill["broaderSkill"])
            result.append(skill_dict)
        return result

    def _process_broader_occupations(self, broader_occupations):
        """Helper method to process broader occupations hierarchy"""
        if not broader_occupations:
            return []
        
        result = []
        for occupation in broader_occupations:
            occupation_dict = {
                "uri": occupation["conceptUri"],
                "label": occupation["preferredLabel_en"],
                "code": occupation.get("code")
            }
            if "broaderOccupation" in occupation:
                occupation_dict["broaderOccupations"] = self._process_broader_occupations(occupation["broaderOccupation"])
            result.append(occupation_dict)
        return result

    def _process_skills(self, skills):
        """Helper method to process skills with their hierarchies and collections"""
        if not skills:
            return []
        
        result = []
        for skill in skills:
            skill_dict = {
                "uri": skill["conceptUri"],
                "label": skill["preferredLabel_en"],
                "skillType": skill.get("skillType")
            }
            result.append(skill_dict)
        return result

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