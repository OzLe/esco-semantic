```markdown
# Weaviate Schema for ESCO Taxonomy (Based on Provided CSV Data)

## 1. Introduction

This document defines a Weaviate schema for representing a subset of the European Skills, Competences, Qualifications and Occupations (ESCO) taxonomy. The schema is specifically tailored to model data derived from the following types of ESCO CSV files:

* Occupation data (e.g., `occupations_en.csv`)
* Skill and competence data (e.g., `skills_en.csv`, `skillGroups_en.csv`)
* ISCO group information (e.g., `ISCOGroups_en.csv`)
* Hierarchical relationships for occupations and skills (e.g., `broaderRelationsOccPillar_en.csv`, `broaderRelationsSkillPillar_en.csv`, `skillsHierarchy_en.csv`)
* Occupation-skill relationships (e.g., `occupationSkillRelations_en.csv`)
* Skill-skill relationships (e.g., `skillSkillRelations_en.csv`)
* Thematic skill collections and concept schemes (e.g., `digCompSkillsCollection_en.csv`, `digitalSkillsCollection_en.csv`, `greenSkillsCollection_en.csv`, `languageSkillsCollection_en.csv`, `researchSkillsCollection_en.csv`, `transversalSkillsCollection_en.csv`, `conceptSchemes_en.csv`)

This schema focuses on creating a knowledge graph of **Occupations**, **Skills**, **ISCO Groups**, and **Skill Collections**, along with their intricate relationships. It does not include ESCO Qualifications or Awarding Bodies, as these were not indicated in the initially provided file list.

## 2. General Weaviate Notes

* **Class Names:** PascalCase (e.g., `Occupation`).
* **Property Names:** camelCase (e.g., `preferredLabelEn`). (Note: Using `_en` suffix in previous examples, but pure camelCase like `preferredLabelEn` is also common. Sticking to `_en` for clarity with ESCO's multilingual nature for this document).
* **Cross-references:** Defined with `dataType: ["TargetClassName"]`. This supports single or multiple references. Inverse relationships should be populated during data import for bidirectional traversal.
* **Vectorization:** Each class should be configured with a vectorizer (e.g., `text2vec-transformers`, `text2vec-openai`) to enable semantic search. Textual properties intended for semantic search should be marked for vectorization. Non-textual IDs, codes, and direct relationship beacons are typically not vectorized.
* **Multilingual Properties:** Textual properties that are language-dependent (e.g., labels, descriptions) are represented with a language suffix (e.g., `_en` for English). This schema primarily outlines English properties; extend as needed for other languages (e.g., `preferredLabel_fr`, `description_de`).
* **Primary Identifier:** The `uri` property, derived from ESCO's unique resource identifiers, is intended to be the conceptual primary key for each object. Weaviate will assign its own UUID to each object, but the `uri` should be stored and indexed for external referencing and data integrity.

## 3. Class Definitions

### 3.1. Occupation Class

* **Class Name:** `Occupation`
* **Description:** Represents an ESCO occupation concept.
* **Recommended Vectorizer:** e.g., `text2vec-transformers` (multilingual model recommended)
* **Source CSV Examples:** `occupations_en.csv`

**Properties:**

| Property Name         | Data Type        | Description                                                                                                     | Vectorized | Source CSV Example(s)       |
|-----------------------|------------------|-----------------------------------------------------------------------------------------------------------------|------------|-----------------------------|
| `uri`                 | `text`           | Unique ESCO URI for the occupation.                                                                             | No         | `occupations_en.csv`        |
| `code`                | `text`           | ESCO code for the occupation.                                                                                   | No         | `occupations_en.csv`        |
| `preferredLabel_en`   | `text`           | Preferred label in English. (Add other languages as `preferredLabel_xx`)                                        | Yes        | `occupations_en.csv`        |
| `altLabels_en`        | `text[]`         | Alternative labels/synonyms in English. (Add other languages as `altLabels_xx`)                                   | Yes        | `occupations_en.csv`        |
| `description_en`      | `text`           | Description in English. (Add other languages as `description_xx`)                                                 | Yes        | `occupations_en.csv`        |
| `definition_en`       | `text`           | Formal definition in English, if available. (Add other languages as `definition_xx`)                              | Yes        | `occupations_en.csv`        |
| `memberOfISCOGroup`   | `["ISCOGroup"]`  | **Relationship**: The ISCO group this occupation belongs to.                                                      | N/A        | `occupations_en.csv` (links to `ISCOGroups_en.csv`) |
| `hasEssentialSkill`   | `["Skill"]`      | **Relationship**: Essential skills for this occupation.                                                         | N/A        | `occupationSkillRelations_en.csv` |
| `hasOptionalSkill`    | `["Skill"]`      | **Relationship**: Optional skills for this occupation.                                                          | N/A        | `occupationSkillRelations_en.csv` |
| `broaderOccupation`   | `["Occupation"]` | **Relationship**: Broader occupation(s) in the hierarchy.                                                       | N/A        | `broaderRelationsOccPillar_en.csv` |
| `narrowerOccupation`  | `["Occupation"]` | **Relationship**: Narrower occupation(s) in the hierarchy (inverse of `broaderOccupation`).                     | N/A        | (Constructed from `broaderRelationsOccPillar_en.csv`) |

---

### 3.2. Skill Class

* **Class Name:** `Skill`
* **Description:** Represents an ESCO skill, competence, or knowledge concept. Includes skill groups as higher-level skills.
* **Recommended Vectorizer:** e.g., `text2vec-transformers` (multilingual model recommended)
* **Source CSV Examples:** `skills_en.csv`, `skillGroups_en.csv`

**Properties:**

| Property Name                 | Data Type        | Description                                                                                                                               | Vectorized | Source CSV Example(s)       |
|-------------------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------|------------|-----------------------------|
| `uri`                         | `text`           | Unique ESCO URI for the skill.                                                                                                            | No         | `skills_en.csv`             |
| `code`                        | `text`           | ESCO code for the skill.                                                                                                                  | No         | `skills_en.csv`             |
| `preferredLabel_en`           | `text`           | Preferred label in English. (Add other languages as `preferredLabel_xx`)                                                                  | Yes        | `skills_en.csv`             |
| `altLabels_en`                | `text[]`         | Alternative labels/synonyms in English. (Add other languages as `altLabels_xx`)                                                           | Yes        | `skills_en.csv`             |
| `description_en`              | `text`           | Description in English. (Add other languages as `description_xx`)                                                                           | Yes        | `skills_en.csv`             |
| `definition_en`               | `text`           | Formal definition in English, if available. (Add other languages as `definition_xx`)                                                        | Yes        | `skills_en.csv`             |
| `skillType`                   | `text`           | Type of skill (e.g., 'skill/competence', 'knowledge', 'language skill', 'digital skill', 'skill group').                                  | No (Filterable) | `skills_en.csv`             |
| `reuseLevel`                  | `text`           | Reusability level (e.g., 'cross-sectoral', 'sector-specific').                                                                            | No (Filterable) | `skills_en.csv`             |
| `isEssentialForOccupation`    | `["Occupation"]` | **Relationship**: Occupations for which this skill is essential (inverse of `hasEssentialSkill` on Occupation).                           | N/A        | (Constructed from `occupationSkillRelations_en.csv`) |
| `isOptionalForOccupation`     | `["Occupation"]` | **Relationship**: Occupations for which this skill is optional (inverse of `hasOptionalSkill` on Occupation).                             | N/A        | (Constructed from `occupationSkillRelations_en.csv`) |
| `broaderSkill`                | `["Skill"]`      | **Relationship**: Broader skill(s) in the hierarchy.                                                                                      | N/A        | `broaderRelationsSkillPillar_en.csv`, `skillsHierarchy_en.csv` |
| `narrowerSkill`               | `["Skill"]`      | **Relationship**: Narrower skill(s) in the hierarchy (inverse of `broaderSkill`).                                                         | N/A        | (Constructed from hierarchy CSVs) |
| `memberOfSkillCollection`     | `["SkillCollection"]` | **Relationship**: Collections this skill belongs to.                                                                                     | N/A        | `*SkillsCollection_en.csv` (e.g., `digitalSkillsCollection_en.csv`) |
| `hasRelatedSkill`             | `["Skill"]`      | **Relationship**: Other non-hierarchical related skills. The nature of the relation might need further modeling if diverse.               | N/A        | `skillSkillRelations_en.csv` |

---

### 3.3. ISCOGroup Class

* **Class Name:** `ISCOGroup`
* **Description:** Represents an ISCO (International Standard Classification of Occupations) group.
* **Recommended Vectorizer:** e.g., `text2vec-transformers`
* **Source CSV Examples:** `ISCOGroups_en.csv`

**Properties:**

| Property Name         | Data Type        | Description                                                                                                     | Vectorized | Source CSV Example(s)       |
|-----------------------|------------------|-----------------------------------------------------------------------------------------------------------------|------------|-----------------------------|
| `uri`                 | `text`           | Unique ESCO URI for the ISCO group.                                                                             | No         | `ISCOGroups_en.csv`         |
| `code`                | `text`           | ISCO code for the group.                                                                                        | No         | `ISCOGroups_en.csv`         |
| `preferredLabel_en`   | `text`           | Preferred label in English. (Add other languages as `preferredLabel_xx`)                                        | Yes        | `ISCOGroups_en.csv`         |
| `description_en`      | `text`           | Description in English. (Add other languages as `description_xx`)                                                 | Yes        | `ISCOGroups_en.csv`         |
| `hasOccupation`       | `["Occupation"]` | **Relationship**: Occupations belonging to this ISCO group (inverse of `memberOfISCOGroup` on Occupation).        | N/A        | (Constructed from `occupations_en.csv`) |
---

### 3.4. SkillCollection Class

* **Class Name:** `SkillCollection`
* **Description:** Represents a thematic collection or concept scheme of skills.
* **Recommended Vectorizer:** e.g., `text2vec-transformers`
* **Source CSV Examples:** `conceptSchemes_en.csv` (for collection metadata), `*SkillsCollection_en.csv` (for members)

**Properties:**

| Property Name         | Data Type        | Description                                                                                                     | Vectorized | Source CSV Example(s)       |
|-----------------------|------------------|-----------------------------------------------------------------------------------------------------------------|------------|-----------------------------|
| `uri`                 | `text`           | Unique URI for the skill collection/concept scheme.                                                             | No         | `conceptSchemes_en.csv`     |
| `preferredLabel_en`   | `text`           | Preferred label of the collection in English. (Add other languages as `preferredLabel_xx`)                        | Yes        | `conceptSchemes_en.csv`     |
| `description_en`      | `text`           | Description of the collection in English. (Add other languages as `description_xx`)                               | Yes        | `conceptSchemes_en.csv`     |
| `hasSkill`            | `["Skill"]`      | **Relationship**: Skills that are part of this collection (inverse of `memberOfSkillCollection` on Skill).        | N/A        | `*SkillsCollection_en.csv` files |

## 4. Data Import Considerations

* **Order of Import:** It's generally advisable to import entities first (Occupations, Skills, ISCOGroups, SkillCollections) and then establish relationships.
* **UUIDs:** Weaviate auto-generates UUIDs. Ensure your import process maps ESCO URIs to these Weaviate UUIDs if you need to resolve relationships using the original ESCO URIs. Storing the ESCO `uri` as a property is crucial.
* **Inverse Relationships:** Populate inverse relationship properties (e.g., `narrowerOccupation`, `isEssentialForOccupation`) during your data import logic by processing the primary relationship data.
* **`skillSkillRelations_en.csv`:** This file might contain various types of relationships. The `hasRelatedSkill` property is a generic placeholder. If the `relationType` column in this CSV is important, you may need to:
    * Create distinct relationship properties in the `Skill` class for each type (e.g., `isPrerequisiteForSkill`, `sharesCompetenceWithSkill`).
    * Or, store the `relationType` as a property on an intermediate "edge" object if using a more graph-native approach (more complex in Weaviate without explicit edge objects). For most Weaviate use cases, distinct properties or relying on the vector semantics of related skills is common.
* **Skill Groups:** Skills from `skillGroups_en.csv` can be imported as instances of the `Skill` class, potentially setting their `skillType` property to "skill group" or a similar identifier. Their hierarchical position will be defined by the broader/narrower relations.

This schema provides a robust starting point for modeling your ESCO CSV data within Weaviate, enabling powerful semantic search and graph-based data exploration.
```