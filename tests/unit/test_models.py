import pytest
from esco.models.skill import Skill
from esco.models.occupation import Occupation
from esco.models.isco_group import ISCOGroup
from esco.models.skill_collection import SkillCollection

class TestSkillModel:
    def test_skill_creation(self):
        skill = Skill(
            uri="test-skill-001",
            preferred_label="Python Programming",
            description="Programming in Python",
            skill_type="technical"
        )
        assert skill.uri == "test-skill-001"
        assert skill.preferred_label == "Python Programming"
        assert skill.validate()
    
    def test_skill_to_dict(self):
        skill = Skill(
            uri="test-skill-001",
            preferred_label="Python Programming"
        )
        data = skill.to_dict()
        assert data['conceptUri'] == "test-skill-001"
        assert data['preferredLabel_en'] == "Python Programming"

class TestOccupationModel:
    def test_occupation_creation(self):
        occupation = Occupation(
            uri="test-occ-001",
            preferred_label="Software Developer",
            description="Develops software applications",
            code="2511"
        )
        assert occupation.uri == "test-occ-001"
        assert occupation.preferred_label == "Software Developer"
        assert occupation.validate()
    
    def test_occupation_to_dict(self):
        occupation = Occupation(
            uri="test-occ-001",
            preferred_label="Software Developer"
        )
        data = occupation.to_dict()
        assert data['conceptUri'] == "test-occ-001"
        assert data['preferredLabel_en'] == "Software Developer"

class TestISCOGroupModel:
    def test_isco_group_creation(self):
        isco_group = ISCOGroup(
            uri="test-isco-001",
            preferred_label="ICT Professionals",
            code="25",
            level=2
        )
        assert isco_group.uri == "test-isco-001"
        assert isco_group.preferred_label == "ICT Professionals"
        assert isco_group.validate()
    
    def test_isco_group_to_dict(self):
        isco_group = ISCOGroup(
            uri="test-isco-001",
            preferred_label="ICT Professionals",
            code="25"
        )
        data = isco_group.to_dict()
        assert data['conceptUri'] == "test-isco-001"
        assert data['preferredLabel_en'] == "ICT Professionals"

class TestSkillCollectionModel:
    def test_skill_collection_creation(self):
        collection = SkillCollection(
            uri="test-collection-001",
            preferred_label="Digital Skills",
            collection_type="digital"
        )
        assert collection.uri == "test-collection-001"
        assert collection.preferred_label == "Digital Skills"
        assert collection.validate()
    
    def test_skill_collection_to_dict(self):
        collection = SkillCollection(
            uri="test-collection-001",
            preferred_label="Digital Skills"
        )
        data = collection.to_dict()
        assert data['conceptUri'] == "test-collection-001"
        assert data['preferredLabel_en'] == "Digital Skills" 