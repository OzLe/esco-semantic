import pandas as pd
from typing import Dict, List, Any

def get_sample_skills() -> List[Dict[str, Any]]:
    """Get sample skill data"""
    return [
        {
            "uri": "skill-001",
            "preferredLabel": "Python Programming",
            "description": "Programming in Python language",
            "altLabels": ["Python coding", "Python development"],
            "skillType": "technical",
            "reuseLevel": "cross-sector"
        },
        {
            "uri": "skill-002",
            "preferredLabel": "Data Analysis",
            "description": "Analyzing and interpreting data",
            "altLabels": ["Data analytics", "Statistical analysis"],
            "skillType": "technical",
            "reuseLevel": "cross-sector"
        },
        {
            "uri": "skill-003",
            "preferredLabel": "Project Management",
            "description": "Managing projects and teams",
            "altLabels": ["Project coordination", "Team leadership"],
            "skillType": "soft",
            "reuseLevel": "cross-sector"
        }
    ]

def get_sample_occupations() -> List[Dict[str, Any]]:
    """Get sample occupation data"""
    return [
        {
            "uri": "occ-001",
            "preferredLabel": "Software Developer",
            "description": "Develops software applications",
            "altLabels": ["Programmer", "Coder"],
            "code": "2511",
            "iscoGroup": "2511",
            "essentialSkills": ["skill-001", "skill-002"],
            "optionalSkills": ["skill-003"]
        },
        {
            "uri": "occ-002",
            "preferredLabel": "Data Scientist",
            "description": "Analyzes and interprets complex data",
            "altLabels": ["Data Analyst", "Data Engineer"],
            "code": "2512",
            "iscoGroup": "2512",
            "essentialSkills": ["skill-002"],
            "optionalSkills": ["skill-001", "skill-003"]
        }
    ]

def get_sample_isco_groups() -> List[Dict[str, Any]]:
    """Get sample ISCO group data"""
    return [
        {
            "uri": "isco-2511",
            "preferredLabel": "Software Developers",
            "description": "Professionals who develop software",
            "code": "2511",
            "parentGroup": "251"
        },
        {
            "uri": "isco-2512",
            "preferredLabel": "Data Scientists",
            "description": "Professionals who analyze data",
            "code": "2512",
            "parentGroup": "251"
        }
    ]

def get_sample_skill_collections() -> List[Dict[str, Any]]:
    """Get sample skill collection data"""
    return [
        {
            "uri": "collection-001",
            "preferredLabel": "Programming Skills",
            "description": "Collection of programming-related skills",
            "skills": ["skill-001"],
            "type": "technical"
        },
        {
            "uri": "collection-002",
            "preferredLabel": "Data Skills",
            "description": "Collection of data-related skills",
            "skills": ["skill-002"],
            "type": "technical"
        }
    ]

def get_sample_dataframes() -> Dict[str, pd.DataFrame]:
    """Get sample data as DataFrames"""
    return {
        "skills": pd.DataFrame(get_sample_skills()),
        "occupations": pd.DataFrame(get_sample_occupations()),
        "isco_groups": pd.DataFrame(get_sample_isco_groups()),
        "skill_collections": pd.DataFrame(get_sample_skill_collections())
    }

def get_sample_search_results() -> List[Dict[str, Any]]:
    """Get sample search results"""
    return [
        {
            "uri": "skill-001",
            "preferredLabel": "Python Programming",
            "description": "Programming in Python language",
            "score": 0.95,
            "type": "skill"
        },
        {
            "uri": "skill-002",
            "preferredLabel": "Data Analysis",
            "description": "Analyzing and interpreting data",
            "score": 0.85,
            "type": "skill"
        }
    ]

def get_sample_translations() -> List[Dict[str, str]]:
    """Get sample translations"""
    return [
        {
            "en": "Python Programming",
            "es": "Programación en Python",
            "fr": "Programmation Python",
            "de": "Python-Programmierung"
        },
        {
            "en": "Data Analysis",
            "es": "Análisis de Datos",
            "fr": "Analyse de Données",
            "de": "Datenanalyse"
        }
    ] 