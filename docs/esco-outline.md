ESCO v1.2 Structural Reference Report

(release 15 May 2024, latest major version at time of writing)

⸻

1. Pillars and Top-Level Concept Schemes

Pillar	Definition	Key concept-scheme URI	CSV artifacts
Occupations	Set of occupations aligned to ISCO-08 codes	http://data.europa.eu/esco/isco	occupations, ISCOGroups, broaderRelationsOccPillar
Skills & Competences	13 500+ knowledge, skill and competence concepts organised in a mono-hierarchy with four sub-classifications (Knowledge, Language skills and knowledge, Skills, Transversal skills)	http://data.europa.eu/esco/skill	skills, skillGroups, skillsHierarchy, languageSkillsCollection, thematic SkillsCollection files, broaderRelationsSkillPillar, skillSkillRelations
Qualifications	Learning outcomes and credentials mapped indirectly to occupations and skills. Distributed in a separate register, not in the 16 standard CSV files.	http://data.europa.eu/esco/qualification	not in language–specific download

￼

⸻

2. File Catalogue (language-specific download, 16 CSV)

CSV file	Entity(ies) conveyed	Relation(s) contained	Typical columns*
occupations_[lang].csv	Occupation	—	conceptUri, preferredLabel, iscoGroup
ISCOGroups_[lang].csv	ISCOGroup	part-of ISCO hierarchy	conceptUri, code, preferredLabel
broaderRelationsOccPillar_[lang].csv	Occ/ISCOGroup ↔ broader Occ/ISCOGroup	skos:broader	conceptUri, broaderUri
skills_[lang].csv	Skill (leaf)	—	conceptUri, preferredLabel, skillType
skillGroups_[lang].csv	SkillGroup (levels 1-3)	—	conceptUri, preferredLabel
skillsHierarchy_[lang].csv	Skill/SkillGroup hierarchy	skos:broader	conceptUri, broaderUri
broaderRelationsSkillPillar_[lang].csv	Additional broader links where needed	skos:broader	conceptUri, broaderUri
occupationSkillRelations_[lang-independent].csv	Occupation ↔ Skill	hasEssentialSkill / hasOptionalSkill via relationType	occupationUri, skillUri, relationType, skillType
skillSkillRelations_[lang-independent].csv	Skill ↔ Skill	relatedSkill plus relation role	skillUri, relatedSkillUri, relationType
languageSkillsCollection_[lang].csv	SkillCollection ↔ Skill	hasSkill / memberOfSkillCollection	collectionUri, skillUri
Thematic SkillsCollection files (greenSkillsCollection, digCompSkillsCollection, etc.)	Same pattern as above	Same	collectionUri, skillUri
researchOccupationsCollection_[lang], researchSkillsCollection_[lang]	Collections for Research Comp framework	Same	collectionUri, memberUri
transversalSkillsCollection_[lang], digitalSkillsCollection_[lang]	Collections for transversal & digital skills	Same	collectionUri, memberUri

*Column names may vary slightly across minor versions; see raw file headers for confirmation.
￼

⸻

3. Core Entity Definitions
	•	Occupation – skos:Concept subclass esco:Occupation; mapped one-to-one to an ISCO-08 code; linked to skills through essential and optional relations.
	•	ISCOGroup – the four top ISCO levels providing the backbone for the occupation hierarchy.
	•	Skill / Competence / Knowledge – skos:Concept subclass esco:Skill; each carries a skillType and skillReuseLevel classification.
	•	SkillGroup – the first three hierarchy levels of the skills pillar (also represented as esco:Skill).
	•	SkillCollection – a labelled subset of skills gathered by a thematic criterion, modelled with esco:Structure. Example: languageSkillsCollection.
	•	ConceptScheme – container of an entire pillar or external framework (e.g. ISCO, DigComp).

⸻

4. Relationship Taxonomy and Supporting CSVs

Source entity	Target entity	Ontology property	CSV that instantiates it
Occupation	ISCOGroup	esco:memberOfGroup (implicit through iscoGroup code)	occupations + ISCOGroups
Occupation	broader Occupation / ISCOGroup	skos:broader	broaderRelationsOccPillar
Skill / SkillGroup	broader Skill / SkillGroup	skos:broader	skillsHierarchy, broaderRelationsSkillPillar
Occupation	Skill	esco:hasEssentialSkill or esco:hasOptionalSkill (direction reversed with isEssentialSkillFor)	occupationSkillRelations (uses relationType = essential / optional)
Skill	related Skill	esco:relatedSkill (semantically typed in relationType)	skillSkillRelations
SkillCollection	Skill	esco:hasSkill	The twelve SkillsCollection CSVs
Skill	SkillCollection	esco:memberOfSkillCollection	same CSVs, inverse
Skill / Occupation / Collection	ConceptScheme	skos:inScheme (not in CSV, present in RDF download)	—

Ontology properties are defined in the ESCO OWL model v2.0.0.

⸻

5. ASCII Entity-Relationship Overview

                +-----------------+           skos:broader
  ISCOGroup  <──┤ Occupation      ├────────┐
                +-----------------+        │ hasEssentialSkill / hasOptionalSkill
                           ▲               │
                           │ skos:broader  │
                +-----------------+        ▼
                |  Occupation     |   +-----------------+
                |  (level ≥5)     |   |     Skill       |
                +-----------------+   +-----------------+
                                              ▲
                skos:broader                  │ skos:broader
                                              │
  SkillCollection ───── hasSkill ─────────────┘
        ▲
        │ memberOfSkillCollection
  +-----------------+
  |  Skill          |
  |  (leaf)         |
  +-----------------+


⸻

6. Validation Evidence
	1.	Completeness of file set – EU Commission download page lists the same 16 CSVs grouped into occupation, skill and relationship categories.  ￼
	2.	Ontology alignment – ESCO ontology defines hasEssentialSkill, hasOptionalSkill, isEssentialSkillFor, skos:broader, hasSkill, matching exactly the CSV relation files.
	3.	Field-level match – community ingestion scripts show occupationSkillRelations.csv headers occupationUri, skillUri, relationType, which are loaded into Neo4j as “ESSENTIAL_FOR” or “OPTIONAL_FOR” edges, mirroring ontology roles.
	4.	Skills pillar hierarchy – the structure page confirms that the pillar is a mono-hierarchy constructed with skillGroups, skillsHierarchy and broader relations.  ￼

All relations described in Sections 3–4 are therefore observable both in the authoritative ontology and in the language-specific CSV deliverables distributed with ESCO v1.2, ensuring internal consistency.

⸻

7. Practical Integration Checklist

Step	Action
1	Load core entities: occupations, ISCOGroups, skills, skillGroups.
2	Apply hierarchical edges from broaderRelationsOccPillar and skillsHierarchy (plus broaderRelationsSkillPillar for edge cases).
3	Build occupation–skill edges with occupationSkillRelations; translate relationType into two edge labels if your store supports typed relations.
4	Optionally enrich skills with thematic collections by loading each SkillsCollection file and linking via hasSkill.
5	Keep URIs as primary keys – they are language-independent and used across all relation files.
6	For cross-language interfaces, left-join additional skills_[lang].csv or occupations_[lang].csv on the same URIs to display labels.


⸻

8. Conclusion

The ESCO data model is a SKOS-compliant, three-pillar taxonomy whose CSV distribution exposes:
	•	Hierarchy through skos:broader relations in separate broader-relation files.
	•	Cross-pillar linkage through typed Association properties captured in occupationSkillRelations and skillSkillRelations.
	•	Thematic views via SkillCollection files that tag targeted subsets such as language, digital or green skills.

These artefacts map one-for-one to the object properties defined in the ESCO OWL ontology. Implementers can therefore reconstruct the full knowledge graph – or any language-specific slice – using only the 16 CSVs plus the ontology as a reference specification, confident that every connection is formally validated by the European Commission’s canonical model.