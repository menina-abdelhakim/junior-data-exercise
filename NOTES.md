# NOTES.md – Pipeline FHIR Patient (Bronze / Silver / Gold)

# Commandes de compilation et d'exécution

Compilez le projet avec sbt, puis exécutez les trois jobs Spark dans l’ordre (Bronze → Silver → Gold).

```bash
sbt clean compile package

spark-submit \
  --class BronzeLayer \
  --master local[*] \
  target/scala-2.12/*.jar

spark-submit \
  --class SilverLayer \
  --master local[*] \
  target/scala-2.12/*.jar

spark-submit \
  --class GoldLayer \
  --master local[*] \
  target/scala-2.12/*.jar
```
## 1. Hypothèses et arbitrages

### Sources et référentiel
- **IPP principal** : la table `identifiants_ipp.csv` est la source de vérité pour le cycle de vie des IPP. Un IPP déprécié sans `ipp_principal` est considéré comme son propre principal (fallback).  
- **Enregistrement le plus récent** : pour chaque `patient_id`, on sélectionne l’enregistrement avec la priorité suivante :  
  1. IPP actif (`is_deprecated = false`)  
  2. `date_fin_validite` la plus récente (les valeurs nulles sont traitées comme une date très lointaine `9999-12-31` car elles indiquent une validité perpétuelle).  

### Nettoyage des données
- **Sexe** : valeurs variées (`Homme`, `male`, `1`, `Femme`, `female`, `2`) mappées vers `H` ou `F` ; les autres valeurs deviennent `null` (qui seront transformées en `unknown` en FHIR).  
- **Prénoms** : la colonne brute contient des chaînes de type `["Jean","Marie"]`. On extrait les prénoms, on les capitalise (première lettre majuscule, gestion des prénoms composés comme `Anne-Claire`) et on dédoublonne le tableau. Si aucun prénom, on met `["UNKNOWN"]` pour respecter la cardinalité FHIR.  
- **Dates** : plusieurs formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MM-yyyy`, `yyyy/MM/dd`) sont normalisés en `yyyy-MM-dd`.  
- **Opposition** : les libellés incohérents (`oui`, `true`, `1`, `Opposé`, `non`, `false`, `0`) sont normalisés en `O` ou `N` ; les absences ou valeurs non reconnues sont traitées comme `false` en Gold.

### Conformité FHIR R4
- **Structure** : la ressource Patient inclut les champs obligatoires (`resourceType`, `id`, `identifier`, `name`), ainsi que les champs recommandés (`text` pour la narrative).  
- **Codages** :  
  - `gender` : `male` | `female` | `unknown`  
  - `address.use` : `home` (pour actuelle ou domicile) et `old` (pour ancienne).  
  - `identifier.system` : une URI réelle (`http://fhir.healthdata.fr/sid/ipp`) pour éviter le domaine réservé `example.org`.  
- **Extension opposition** : incluse uniquement si `hasOpposition = true`, avec l'URL `http://fhir.healthdata.fr/StructureDefinition/opposition`.  
- **Narrative** : un champ `text` minimal (`status = generated`) est ajouté pour satisfaire la recommandation `dom-6` (meilleure pratique FHIR).  

### Format de sortie
- **JSON Lines** : chaque ligne est un objet JSON représentant une ressource Patient. Ce format est léger, facile à ingérer par une API REST, et permet un traitement en streaming ou batch sans retraitement supplémentaire.

---

## 2. Anomalies détectées dans les données sources et traitements appliqués

| Anomalie | Traitement |
|----------|------------|
| **Dates** dans plusieurs formats | Normalisation vers `yyyy-MM-dd` via une fonction Spark testant les motifs principaux. |
| **Sexe** : valeurs libres (`Homme`, `male`, `1`, etc.) | Mapping vers `H` ou `F` ; valeurs non reconnues → `null` (qui devient `"unknown"`). |
| **Prénoms** : chaînes avec crochets, guillemets, séparateurs virgules, espaces superflus | Extraction, nettoyage, capitalisation, dédoublonnage interne. Résultat = tableau. |
| **Opposition** : libellés incohérents (`oui`, `true`, `1`, `Opposé`, `non`, `false`, `0`) | Normalisation en `O` ou `N` ; valeurs non reconnues → `null` (interprété comme `false` en Gold). |
| **Adresses en double** pour un même patient (ex: `"2 Rue du Port"` vs `"2 rue du Port"`) | Normalisation de la ligne (majuscules, espaces uniques) + `collect_set` et `array_distinct` pour éliminer les strictes doublons. |
| **Adresses sans type** (patient `800000132`) | Défaut `use = home` lors du mapping FHIR. |
| **Patients sans nom de naissance** | Filtrage en Gold (`nom_naissance IS NOT NULL`) car `family` est obligatoire en FHIR. |
| **Patient `800000137` inexistant** apparu avec `fhir_json: null` | Ce patient n'existe pas dans les données sources ; le filtre sur `nom_naissance` l'exclut. |
| **IPP dépréciés sans `ipp_principal`** | Considérés comme leur propre principal (fallback). |

---

## 3. Pistes d’amélioration (avec plus de temps)

- **Qualité des données** : ajouter des contrôles de qualité à chaque couche (nombre de lignes, valeurs nulles, cohérence des dates) et générer des rapports d’anomalies.  
- **Tests unitaires** : mettre en place des tests sur les transformations Spark (ex: avec `spark-testing-base`) pour valider les cas critiques.  
- **Gestion des télécoms** : si les données sources incluent téléphone ou email, les intégrer dans la ressource FHIR (`telecom`).  
- **Identifiants multiples** : permettre d’autres identifiants (NIR, ID externe) dans le tableau `identifier`.  
- **Externalisation des mappings** : stocker les correspondances (sexe, opposition, type d’adresse) dans des fichiers de référence pour faciliter la maintenance.  
- **Optimisation des performances** : pour de gros volumes, envisager le partitionnement par `patient_id` et l’utilisation de broadcast joins.  
- **Validation automatique** : intégrer un validateur FHIR (ex: HAPI) dans le pipeline pour s’assurer de la conformité du JSON produit avant écriture.  
- **API directe** : exposer le résultat via une API REST (ex: avec Spark Structured Streaming et un serveur HTTP) plutôt que des fichiers batch.

---

## 4. Échantillon de la table Patient produite (format JSON Lines)

Ci-dessous un exemple de ressource Patient générée par le pipeline (fichier `part-00000` dans `data/gold/patients_fhir/`).

```json
{"resourceType":"Patient","id":"800000123","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000123"}],"active":true,"name":[{"use":"official","family":"MARTIN","given":["Jean"]}],"gender":"male","birthDate":"1985-03-12","address":[{"line":"12 RUE DE LA PAIX","postalCode":"75002","city":"PARIS","country":"FRANCE","use":"home"},{"line":"3 ALLÉE DES TILLEULS","postalCode":"69003","city":"LYON","country":"FRANCE","use":"old"}]}
{"resourceType":"Patient","id":"800000124","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000124"}],"active":true,"name":[{"use":"official","family":"BERNARD","given":["Marie","Claire"]}],"gender":"female","birthDate":"1990-07-23","address":[{"line":"45 AVENUE VICTOR HUGO","postalCode":"33000","city":"BORDEAUX","country":"FRANCE","use":"home"}],"extension":[{"url":"http://fhir.healthdata.fr/StructureDefinition/opposition","valueBoolean":true}]}
{"resourceType":"Patient","id":"800000125","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000125"}],"active":true,"name":[{"use":"official","family":"PETIT","given":["Lucas","Gabriel"]}],"gender":"male","birthDate":"2001-11-02"}
{"resourceType":"Patient","id":"800000126","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000126"}],"active":true,"name":[{"use":"official","family":"EL AMRANI","given":["Fatima"]}],"gender":"female","birthDate":"1978-02-28","address":[{"line":"7 BOULEVARD HAUSSMANN","postalCode":"75009","city":"PARIS","country":"FRANCE","use":"home"},{"line":"7 BD HAUSSMANN","postalCode":"75009","city":"PARIS","use":"old"}],"extension":[{"url":"http://fhir.healthdata.fr/StructureDefinition/opposition","valueBoolean":true}]}
{"resourceType":"Patient","id":"800000127","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000127"}],"active":false,"name":[{"use":"official","family":"LE GALL","given":["Pierre-Yves"]}],"gender":"male","birthDate":"1965-09-30","deceasedDateTime":"2023-04-15","address":[{"line":"2 RUE DU PORT","postalCode":"29200","city":"BREST","country":"FRANCE","use":"home"}]}
{"resourceType":"Patient","id":"800000128","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000128"}],"active":true,"name":[{"use":"official","family":"ROUX","given":["Sophie","Marie","Louise"]}],"gender":"female","birthDate":"1982-12-05","address":[{"line":"18 RUE DES LILAS","postalCode":"59000","city":"LILLE","country":"FRANCE","use":"home"},{"line":"90 RUE NATIONALE","postalCode":"59000","city":"LILLE","country":"FRANCE","use":"old"}],"extension":[{"url":"http://fhir.healthdata.fr/StructureDefinition/opposition","valueBoolean":true}]}
{"resourceType":"Patient","id":"800000129","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000129"}],"active":true,"name":[{"use":"official","family":"BENALI","given":["Mohammed","Amine"]}],"gender":"male","birthDate":"1995-04-17","address":[{"line":"5 IMPASSE DES ROSES","city":"MARSEILLE","country":"FRANCE","use":"home"}]}
{"resourceType":"Patient","id":"800000130","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000130"}],"active":true,"name":[{"use":"official","family":"MOREAU","given":["Camille"]}],"gender":"unknown","birthDate":"1999-06-21","address":[{"line":"22 QUAI DE SEINE","postalCode":"75019","city":"PARIS","country":"FRANCE","use":"home"}],"extension":[{"url":"http://fhir.healthdata.fr/StructureDefinition/opposition","valueBoolean":true}]}
{"resourceType":"Patient","id":"800000131","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000131"}],"active":true,"name":[{"use":"official","family":"DUBOIS","given":["Anne-Claire"]}],"gender":"female","birthDate":"1973-01-09","address":[{"line":"14 RUE GAMBETTA","postalCode":"44000","city":"NANTES","country":"FRANCE","use":"home"},{"line":"14 R. GAMBETTA","postalCode":"44000","city":"NANTES","country":"FRANCE","use":"old"}],"extension":[{"url":"http://fhir.healthdata.fr/StructureDefinition/opposition","valueBoolean":true}]}
{"resourceType":"Patient","id":"800000132","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000132"}],"active":true,"name":[{"use":"official","family":"ROSSI","given":["Giovanni"]}],"gender":"male","birthDate":"1988-08-14","address":[{"line":"16 RUE DE LA RÉPUBLIQUE","postalCode":"69002","city":"LYON","country":"FRANCE","use":"home"}]}
{"resourceType":"Patient","id":"800000133","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000133"}],"active":true,"name":[{"use":"official","family":"GARCIA","given":["Élodie"]}],"gender":"female","birthDate":"1992-03-03","address":[{"line":"8 COURS MIRABEAU","postalCode":"13100","city":"AIX-EN-PROVENCE","country":"FRANCE","use":"home"}]}
{"resourceType":"Patient","id":"800000134","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000134"}],"active":true,"name":[{"use":"official","family":"LAMBERT","given":["Thomas"]}],"gender":"male","address":[{"line":"3 PLACE BELLECOUR","postalCode":"6900","city":"LYON","country":"FRANCE","use":"home"}]}
{"resourceType":"Patient","id":"800000135","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000135"}],"active":true,"name":[{"use":"official","family":"DIALLO","given":["Aïcha"]}],"gender":"female","birthDate":"2005-05-19","address":[{"line":"11 RUE PASTEUR","postalCode":"67000","city":"STRASBOURG","country":"FRANCE","use":"home"}]}
{"resourceType":"Patient","id":"800000136","text":{"status":"generated","div":"<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"},"identifier":[{"use":"usual","system":"http://fhir.healthdata.fr/sid/ipp","value":"800000136"}],"active":false,"name":[{"use":"official","family":"GIRARD","given":["Robert"]}],"gender":"male","birthDate":"1949-07-07","deceasedDateTime":"2022-11-30","address":[{"line":"221B BAKER STREET","postalCode":"NW1 6XE","city":"LONDRES","country":"ROYAUME-UNI","use":"home"}]}
