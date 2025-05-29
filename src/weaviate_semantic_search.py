import re
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from sentence_transformers import SentenceTransformer
from src.logging_config import setup_logging
from src.esco_weaviate_client import WeaviateClient
import torch
import numpy as np

logger = setup_logging()

@dataclass
class TaxonomyEnrichmentResult:
    """Structured result for taxonomy enrichment"""
    job_title: str
    job_description: str
    matched_occupations: List[Dict[str, Any]]
    extracted_skills: List[Dict[str, Any]]
    skill_gaps: List[Dict[str, Any]]
    isco_groups: List[Dict[str, Any]]
    confidence_score: float
    enrichment_metadata: Dict[str, Any]

@dataclass
class OccupationProfile:
    """Complete occupation profile with related entities"""
    occupation: Dict[str, Any]
    essential_skills: List[Dict[str, Any]]
    optional_skills: List[Dict[str, Any]]
    isco_group: Dict[str, Any]
    broader_occupations: List[Dict[str, Any]]
    narrower_occupations: List[Dict[str, Any]]
    skill_collections: List[Dict[str, Any]]

class JobPostingProcessor:
    """Processes job postings to extract relevant information"""
    
    def __init__(self):
        # Common skill keywords and patterns
        self.skill_patterns = [
            r'\b(?:experience with|knowledge of|proficient in|skilled in|expertise in)\s+([^.,;]+)',
            r'\b(?:must have|required|essential):\s*([^.,;]+)',
            r'\b([A-Za-z\s]+)\s+(?:skills?|experience|knowledge)',
            r'\b(?:programming|coding|development)\s+(?:in|with)?\s*([^.,;]+)',
        ]
        
        # Common requirement indicators
        self.requirement_indicators = [
            'required', 'must have', 'essential', 'mandatory', 
            'minimum', 'at least', 'experience with', 'knowledge of'
        ]
    
    def extract_skills_from_text(self, text: str) -> List[str]:
        """Extract potential skills from job posting text"""
        extracted_skills = set()
        
        # Convert to lowercase for pattern matching
        text_lower = text.lower()
        
        # Apply skill extraction patterns
        for pattern in self.skill_patterns:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            for match in matches:
                # Clean and split the match
                skills = [s.strip() for s in match.split(',') if s.strip()]
                extracted_skills.update(skills)
        
        # Filter out common non-skill terms
        filtered_skills = []
        stop_words = {'and', 'or', 'the', 'a', 'an', 'with', 'in', 'on', 'at', 'for', 'to', 'of'}
        
        for skill in extracted_skills:
            if len(skill) > 2 and skill not in stop_words:
                filtered_skills.append(skill)
        
        return list(set(filtered_skills))
    
    def categorize_requirements(self, text: str) -> Dict[str, List[str]]:
        """Categorize requirements as essential vs optional"""
        essential_terms = ['required', 'must have', 'essential', 'mandatory', 'minimum']
        preferred_terms = ['preferred', 'nice to have', 'bonus', 'plus', 'desirable']
        
        # Simple categorization based on context
        sentences = text.split('.')
        essential_requirements = []
        preferred_requirements = []
        
        for sentence in sentences:
            sentence_lower = sentence.lower()
            if any(term in sentence_lower for term in essential_terms):
                essential_requirements.extend(self.extract_skills_from_text(sentence))
            elif any(term in sentence_lower for term in preferred_terms):
                preferred_requirements.extend(self.extract_skills_from_text(sentence))
        
        return {
            'essential': list(set(essential_requirements)),
            'preferred': list(set(preferred_requirements))
        }

class ESCOSemanticSearch:
    def __init__(self, config_path: str = "config/weaviate_config.yaml", profile: str = "default"):
        """Initialize the semantic search with configuration."""
        self.client = WeaviateClient(config_path, profile)
        self.model = SentenceTransformer('all-MiniLM-L6-v2', device=self._get_device())
        self.job_processor = JobPostingProcessor()
        
        # Initialize repositories
        self.skill_repo = self.client.get_repository("Skill")
        self.occupation_repo = self.client.get_repository("Occupation")
        self.isco_group_repo = self.client.get_repository("ISCOGroup")
        self.skill_collection_repo = self.client.get_repository("SkillCollection")

    def _get_device(self) -> str:
        """Get the best available device for PyTorch."""
        if torch.cuda.is_available():
            return "cuda"
        elif torch.backends.mps.is_available():
            return "mps"
        return "cpu"

    def validate_data(self) -> Tuple[bool, Dict[str, Any]]:
        """Validate the data in the Weaviate database"""
        validation_details = {
            "skills_indexed": False,
            "occupations_indexed": False,
            "isco_groups_indexed": False,
            "skills_count": 0,
            "occupations_count": 0,
            "isco_groups_count": 0,
            "errors": []
        }
        
        try:
            # Check each entity type
            for entity_type in ["Skill", "Occupation", "ISCOGroup"]:
                try:
                    result = self.client.client.query.aggregate(entity_type).with_meta_count().do()
                    count = result["data"]["Aggregate"][entity_type][0]["meta"]["count"]
                    
                    if entity_type == "Skill":
                        validation_details["skills_count"] = count
                        validation_details["skills_indexed"] = count > 0
                    elif entity_type == "Occupation":
                        validation_details["occupations_count"] = count
                        validation_details["occupations_indexed"] = count > 0
                    elif entity_type == "ISCOGroup":
                        validation_details["isco_groups_count"] = count
                        validation_details["isco_groups_indexed"] = count > 0
                        
                except Exception as e:
                    validation_details["errors"].append(f"Error checking {entity_type}: {str(e)}")
            
            is_valid = (validation_details["skills_indexed"] and 
                       validation_details["occupations_indexed"])
            
            return is_valid, validation_details
            
        except Exception as e:
            error_msg = f"Error during data validation: {str(e)}"
            logger.error(error_msg)
            validation_details["errors"].append(error_msg)
            return False, validation_details

    def _execute_weaviate_query(self, query_builder) -> Optional[List[Dict]]:
        """Execute a Weaviate query and return results"""
        try:
            result = query_builder.do()
            return result
        except Exception as e:
            logger.error(f"Error executing Weaviate query: {str(e)}")
            return None

    def search_occupations_by_text(self, query_text: str, limit: int = 10, 
                                 similarity_threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Search for occupations using semantic similarity"""
        try:
            # Generate query embedding
            query_embedding = self.model.encode(query_text).tolist()
            
            # Search for occupations
            result = (
                self.client.client.query
                .get("Occupation", [
                    "conceptUri", "preferredLabel_en", "description_en", 
                    "definition_en", "code", "altLabels_en"
                ])
                .with_near_vector({
                    "vector": query_embedding,
                    "certainty": similarity_threshold
                })
                .with_limit(limit)
                .with_additional(["certainty", "distance"])
                .do()
            )
            
            occupations = result.get("data", {}).get("Get", {}).get("Occupation", [])
            
            # Enrich with additional metadata
            for occupation in occupations:
                occupation["match_type"] = "semantic"
                occupation["similarity_score"] = occupation.get("_additional", {}).get("certainty", 0)
            
            return occupations
            
        except Exception as e:
            logger.error(f"Error searching occupations: {str(e)}")
            return []

    def search_skills_by_text(self, query_text: str, limit: int = 20, 
                            similarity_threshold: float = 0.6) -> List[Dict[str, Any]]:
        """Search for skills using semantic similarity"""
        try:
            # Generate query embedding
            query_embedding = self.model.encode(query_text).tolist()
            
            # Search for skills
            result = (
                self.client.client.query
                .get("Skill", [
                    "conceptUri", "preferredLabel_en", "description_en", 
                    "skillType", "reuseLevel", "altLabels_en"
                ])
                .with_near_vector({
                    "vector": query_embedding,
                    "certainty": similarity_threshold
                })
                .with_limit(limit)
                .with_additional(["certainty", "distance"])
                .do()
            )
            
            skills = result.get("data", {}).get("Get", {}).get("Skill", [])
            
            # Enrich with additional metadata
            for skill in skills:
                skill["match_type"] = "semantic"
                skill["similarity_score"] = skill.get("_additional", {}).get("certainty", 0)
            
            return skills
            
        except Exception as e:
            logger.error(f"Error searching skills: {str(e)}")
            return []

    def get_occupation_profile(self, occupation_uri: str) -> Optional[OccupationProfile]:
        """Get complete profile for an occupation with all related entities"""
        try:
            # Get occupation details
            occupation_result = (
                self.client.client.query
                .get("Occupation", [
                    "conceptUri", "preferredLabel_en", "description_en", 
                    "definition_en", "code", "altLabels_en"
                ])
                .with_where({
                    "path": ["conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .with_additional(["id"])
                .do()
            )
            
            occupations = occupation_result.get("data", {}).get("Get", {}).get("Occupation", [])
            if not occupations:
                return None
            
            occupation = occupations[0]
            occupation_id = occupation["_additional"]["id"]
            
            # Get essential skills
            essential_skills_result = (
                self.client.client.query
                .get("Skill", [
                    "conceptUri", "preferredLabel_en", "description_en", 
                    "skillType", "reuseLevel"
                ])
                .with_where({
                    "path": ["isEssentialForOccupation", "Occupation", "conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .with_additional(["certainty"])
                .do()
            )
            essential_skills = essential_skills_result.get("data", {}).get("Get", {}).get("Skill", [])
            
            # Get optional skills
            optional_skills_result = (
                self.client.client.query
                .get("Skill", [
                    "conceptUri", "preferredLabel_en", "description_en", 
                    "skillType", "reuseLevel"
                ])
                .with_where({
                    "path": ["isOptionalForOccupation", "Occupation", "conceptUri"],
                    "operator": "Equal",
                    "valueString": occupation_uri
                })
                .with_additional(["certainty"])
                .do()
            )
            optional_skills = optional_skills_result.get("data", {}).get("Get", {}).get("Skill", [])
            
            # Get ISCO Group
            isco_group = []
            try:
                isco_result = (
                    self.client.client.query
                    .get("ISCOGroup", [
                        "conceptUri", "preferredLabel_en", "description_en", "code"
                    ])
                    .with_where({
                        "path": ["hasOccupation", "Occupation", "conceptUri"],
                        "operator": "Equal",
                        "valueString": occupation_uri
                    })
                    .do()
                )
                isco_group = isco_result.get("data", {}).get("Get", {}).get("ISCOGroup", [])
            except Exception as e:
                logger.warning(f"Could not fetch ISCO group for {occupation_uri}: {str(e)}")
            
            return OccupationProfile(
                occupation=occupation,
                essential_skills=essential_skills,
                optional_skills=optional_skills,
                isco_group=isco_group[0] if isco_group else {},
                broader_occupations=[],  # Would need additional queries
                narrower_occupations=[],  # Would need additional queries
                skill_collections=[]  # Would need additional queries
            )
            
        except Exception as e:
            logger.error(f"Error getting occupation profile for {occupation_uri}: {str(e)}")
            return None

    def enrich_job_posting(self, job_title: str, job_description: str, 
                          max_occupations: int = 5, max_skills: int = 20) -> TaxonomyEnrichmentResult:
        """
        Main method for enriching a job posting with ESCO taxonomy
        
        Args:
            job_title: The job title
            job_description: Full job description text
            max_occupations: Maximum number of matching occupations to return
            max_skills: Maximum number of matching skills to return
            
        Returns:
            TaxonomyEnrichmentResult with structured enrichment data
        """
        # Validate data first
        is_valid, validation_details = self.validate_data()
        if not is_valid:
            raise ValueError(f"Data validation failed: {validation_details}")
        
        # Extract skills from job description
        extracted_text_skills = self.job_processor.extract_skills_from_text(job_description)
        categorized_requirements = self.job_processor.categorize_requirements(job_description)
        
        # Search for matching occupations
        # Combine job title and description for better matching
        search_text = f"{job_title}. {job_description}"
        matched_occupations = self.search_occupations_by_text(
            search_text, 
            limit=max_occupations,
            similarity_threshold=0.6
        )
        
        # Get detailed profiles for top occupations
        occupation_profiles = []
        for occupation in matched_occupations[:3]:  # Get profiles for top 3
            profile = self.get_occupation_profile(occupation["conceptUri"])
            if profile:
                occupation_profiles.append(profile)
        
        # Search for skills mentioned in the job posting
        all_skills = []
        skill_confidences = {}
        
        # Search for skills based on extracted text
        for skill_text in extracted_text_skills[:10]:  # Limit to avoid too many API calls
            found_skills = self.search_skills_by_text(skill_text, limit=3, similarity_threshold=0.5)
            for skill in found_skills:
                skill_uri = skill["conceptUri"]
                if skill_uri not in skill_confidences:
                    all_skills.append(skill)
                    skill_confidences[skill_uri] = skill["similarity_score"]
                else:
                    # Keep the higher confidence score
                    if skill["similarity_score"] > skill_confidences[skill_uri]:
                        skill_confidences[skill_uri] = skill["similarity_score"]
        
        # Also search based on full job description
        description_skills = self.search_skills_by_text(
            job_description, 
            limit=max_skills, 
            similarity_threshold=0.4
        )
        
        # Combine and deduplicate skills
        combined_skills = {}
        for skill in all_skills + description_skills:
            uri = skill["conceptUri"]
            if uri not in combined_skills or skill["similarity_score"] > combined_skills[uri]["similarity_score"]:
                combined_skills[uri] = skill
        
        extracted_skills = list(combined_skills.values())
        extracted_skills.sort(key=lambda x: x["similarity_score"], reverse=True)
        extracted_skills = extracted_skills[:max_skills]
        
        # Identify skill gaps (skills required by matched occupations but not found in job posting)
        required_skills = set()
        for profile in occupation_profiles:
            for skill in profile.essential_skills:
                required_skills.add(skill["conceptUri"])
        
        found_skill_uris = {skill["conceptUri"] for skill in extracted_skills}
        gap_skill_uris = required_skills - found_skill_uris
        
        skill_gaps = []
        for profile in occupation_profiles:
            for skill in profile.essential_skills:
                if skill["conceptUri"] in gap_skill_uris:
                    skill["gap_type"] = "essential_missing"
                    skill_gaps.append(skill)
        
        # Get ISCO groups from matched occupations
        isco_groups = []
        for profile in occupation_profiles:
            if profile.isco_group:
                isco_groups.append(profile.isco_group)
        
        # Calculate overall confidence score
        occupation_confidence = np.mean([occ["similarity_score"] for occ in matched_occupations]) if matched_occupations else 0
        skill_confidence = np.mean([skill["similarity_score"] for skill in extracted_skills]) if extracted_skills else 0
        overall_confidence = (occupation_confidence + skill_confidence) / 2
        
        # Prepare enrichment metadata
        enrichment_metadata = {
            "extraction_method": "semantic_similarity",
            "model_used": "all-MiniLM-L6-v2",
            "extracted_text_skills": extracted_text_skills,
            "categorized_requirements": categorized_requirements,
            "occupation_profiles_count": len(occupation_profiles),
            "total_skills_found": len(extracted_skills),
            "skill_gaps_count": len(skill_gaps),
            "processing_timestamp": None  # Would add timestamp in real implementation
        }
        
        return TaxonomyEnrichmentResult(
            job_title=job_title,
            job_description=job_description,
            matched_occupations=matched_occupations,
            extracted_skills=extracted_skills,
            skill_gaps=skill_gaps,
            isco_groups=list({g["conceptUri"]: g for g in isco_groups}.values()),  # Deduplicate
            confidence_score=overall_confidence,
            enrichment_metadata=enrichment_metadata
        )

    def batch_enrich_job_postings(self, job_postings: List[Dict[str, str]]) -> List[TaxonomyEnrichmentResult]:
        """
        Batch process multiple job postings
        
        Args:
            job_postings: List of dicts with 'title' and 'description' keys
            
        Returns:
            List of TaxonomyEnrichmentResult objects
        """
        results = []
        for job in job_postings:
            try:
                result = self.enrich_job_posting(
                    job_title=job["title"],
                    job_description=job["description"]
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing job '{job.get('title', 'Unknown')}': {str(e)}")
                # Could add a failed result object here
        
        return results

    def get_enrichment_summary(self, result: TaxonomyEnrichmentResult) -> Dict[str, Any]:
        """Generate a summary of the enrichment results"""
        return {
            "job_title": result.job_title,
            "confidence_score": result.confidence_score,
            "matched_occupations_count": len(result.matched_occupations),
            "top_occupation": result.matched_occupations[0]["preferredLabel_en"] if result.matched_occupations else None,
            "extracted_skills_count": len(result.extracted_skills),
            "skill_gaps_count": len(result.skill_gaps),
            "isco_groups": [g["preferredLabel_en"] for g in result.isco_groups],
            "top_skills": [s["preferredLabel_en"] for s in result.extracted_skills[:5]],
            "critical_missing_skills": [s["preferredLabel_en"] for s in result.skill_gaps[:3]]
        }