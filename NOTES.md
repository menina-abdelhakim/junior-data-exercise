# NOTES.md – Pipeline FHIR Patient

## 1. Hypothèses et arbitrages

- **IPP principal** : la table `identifiants_ipp.csv` est la source de référence pour le cycle de vie des IPP. Un IPP déprécié sans `ipp_principal` est considéré comme son propre principal (fallback).  
- **Validité des enregistrements** : une `date_fin_validite` nulle signifie que l’enregistrement est toujours valide. Dans la sélection du meilleur enregistrement, nous traitons `NULL` comme une date très lointaine (`9999-12-31`) pour prioriser les enregistrements actifs.  
- **Adresses** : les types `actuelle` et `domicile` sont mappés au code FHIR `home` ; `ancienne` est mappé à `old`. Les adresses sans type reçoivent `home` par défaut.  
- **Opposition** : par défaut, un patient est considéré comme non opposé (`hasOpposition = false`). L’extension FHIR n’est incluse que si l’opposition est vraie, conformément aux spécifications FHIR.  
- **Prénoms manquants** : remplacés par `["UNKNOWN"]` pour éviter un tableau vide, ce qui est requis dans la ressource FHIR.  
- **Format de sortie** : JSON Lines (un objet JSON par ligne) car chaque ligne est une ressource autonome, facile à ingérer par une API REST ou à charger dans une base de données orientée documents.  

---

## 2. Anomalies détectées et traitements

| Anomalie | Traitement appliqué |
|----------|---------------------|
| **Dates** dans plusieurs formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MM-yyyy`, `yyyy/MM/dd`) | Normalisation vers le format `yyyy-MM-dd` via une fonction Spark testant plusieurs motifs. |
| **Sexe** : valeurs variées (`Homme`, `male`, `1`, `Femme`, `female`, `2`, etc.) | Mapping vers `H` (homme) ou `F` (femme) ; valeurs non reconnues → `null` (qui devient `"unknown"` dans FHIR). |
| **Prénoms** : chaînes avec crochets, guillemets, séparateurs virgules, espaces superflus | Extraction, nettoyage, capitalisation (première lettre en majuscule, gestion des prénoms composés comme `Anne-Claire`), dédoublonnage interne. Résultat = tableau. |
| **Opposition** : libellés incohérents (`oui`, `true`, `1`, `Opposé`, `non`, `false`, `0`) | Normalisation en `O` ou `N` ; valeurs non reconnues → `null` (interprété comme `false` en Gold). |
| **Doublons d’adresses** pour un même patient (ex: `"2 Rue du Port"` vs `"2 rue du Port"`) | Normalisation de la ligne (majuscules, espaces uniques) + utilisation de `collect_set` et `array_distinct` pour éliminer les strictes doublons. |
| **Patients sans nom de naissance** (cas `800000137` dans le jeu de test) | Filtrage en Gold (`nom_naissance IS NOT NULL`) car le champ `family` est obligatoire en FHIR. |
| **Adresse sans `use`** (patient `800000132`) | Défaut `"home"` lors du mapping. |
| **Date de décès présente** | `active = false` et `deceasedDateTime` renseigné. |

---

## 3. Pistes d’amélioration (avec plus de temps)

- **Qualité des données** : ajouter des contrôles de qualité à chaque couche (nombre de lignes, valeurs nulles, cohérence des dates) et générer des rapports d’anomalies.  
- **Tests unitaires** : mettre en place des tests sur les transformations Spark (ex: avec `spark-testing-base`) pour valider les cas critiques.  
- **Gestion des télécoms** : si les données sources incluent téléphone ou email, les intégrer dans la ressource FHIR (`telecom`).  
- **Identifiants multiples** : permettre d’autres identifiants (NIR, ID externe) dans le tableau `identifier`.  
- **Externalisation des mappings** : stocker les correspondances (sexe, opposition, type d’adresse) dans des fichiers de référence pour faciliter la maintenance.  
- **Optimisation des performances** : pour de gros volumes, envisager le partitionnement par `patient_id` et l’utilisation de broadcast joins.  
- **Validation FHIR** : utiliser un validateur FHIR (ex: HAPI) pour s’assurer automatiquement de la conformité du JSON produit.  
- **API directe** : exposer le résultat via une API REST (ex: avec Spark Structured Streaming et un serveur HTTP) plutôt que des fichiers batch.

---

## 4. Échantillon de la table Patient produite (format JSON Lines)

Voici trois exemples de ressources Patient générées par le pipeline (fichiers `part-*.json` dans `data/gold/patients_fhir/`).

```json
{"patient_id":800000123,"fhir_json":"{\"resourceType\":\"Patient\",\"id\":\"800000123\",\"identifier\":[{\"use\":\"usual\",\"system\":\"http://example.org/fhir/identifier/ipp\",\"value\":\"800000123\"}],\"active\":true,\"name\":[{\"use\":\"official\",\"family\":\"MARTIN\",\"given\":[\"Jean\"]}],\"gender\":\"male\",\"birthDate\":\"1985-03-12\",\"address\":[{\"line\":\"12 RUE DE LA PAIX\",\"postalCode\":\"75002\",\"city\":\"PARIS\",\"country\":\"FRANCE\",\"use\":\"home\"},{\"line\":\"3 ALLÉE DES TILLEULS\",\"postalCode\":\"69003\",\"city\":\"LYON\",\"country\":\"FRANCE\",\"use\":\"old\"}]}"}
{"patient_id":800000124,"fhir_json":"{\"resourceType\":\"Patient\",\"id\":\"800000124\",\"identifier\":[{\"use\":\"usual\",\"system\":\"http://example.org/fhir/identifier/ipp\",\"value\":\"800000124\"}],\"active\":true,\"name\":[{\"use\":\"official\",\"family\":\"BERNARD\",\"given\":[\"Marie\",\"Claire\"]}],\"gender\":\"female\",\"birthDate\":\"1990-07-23\",\"address\":[{\"line\":\"45 AVENUE VICTOR HUGO\",\"postalCode\":\"33000\",\"city\":\"BORDEAUX\",\"country\":\"FRANCE\",\"use\":\"home\"}],\"extension\":[{\"url\":\"http://example.org/fhir/StructureDefinition/opposition\",\"valueBoolean\":true}]}"}
{"patient_id":800000127,"fhir_json":"{\"resourceType\":\"Patient\",\"id\":\"800000127\",\"identifier\":[{\"use\":\"usual\",\"system\":\"http://example.org/fhir/identifier/ipp\",\"value\":\"800000127\"}],\"active\":false,\"name\":[{\"use\":\"official\",\"family\":\"LE GALL\",\"given\":[\"Pierre-Yves\"]}],\"gender\":\"male\",\"birthDate\":\"1965-09-30\",\"deceasedDateTime\":\"2023-04-15\",\"address\":[{\"line\":\"2 RUE DU PORT\",\"postalCode\":\"29200\",\"city\":\"BREST\",\"country\":\"FRANCE\",\"use\":\"home\"}]}"}