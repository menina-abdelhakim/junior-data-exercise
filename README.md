# Pipeline FHIR Patient – Bronze / Silver / Gold

## Présentation

Ce projet implémente un pipeline de traitement de données avec Apache Spark (Scala) permettant de consolider des données patient provenant de plusieurs fichiers CSV (patients, adresses, identifiants IPP et opposition à la recherche) afin de produire des ressources **Patient FHIR R4** prêtes à être exposées via une API.

Le pipeline suit l’architecture **Medallion (Bronze / Silver / Gold)** et génère un résultat final au format **JSON Lines**, où chaque ligne correspond à une ressource Patient FHIR valide.

---

## Objectifs

- Ingérer les données brutes CSV (Bronze)
- Normaliser et nettoyer les données (Silver)
- Consolider les patients via les IPP principaux (Gold)
- Dédupliquer les adresses
- Agréger les oppositions à la recherche
- Produire des ressources Patient conformes à FHIR R4

---

## Architecture du pipeline

```text
resources/*.csv
        │
        ▼
Bronze (CSV → Parquet)
        │
        ▼
Silver (nettoyage, normalisation)
        │
        ▼
Gold (consolidation FHIR)
        │
        ▼
data/gold/patients_fhir/
```

## Structure du projet
```text
projet/
├── build.sbt
├── src/
│   └── main/
│       └── scala/
│           ├── BronzeLayer.scala
│           ├── SilverLayer.scala
│           └── GoldLayer.scala
├── resources/
│   ├── patients.csv
│   ├── adresses.csv
│   ├── identifiants_ipp.csv
│   └── opposition_recherche.csv
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       └── patients_fhir/
├── README.md
└── NOTES.md
```

## Prérequis

* Java 17
* Apache Spark 3.5.x
* Scala 2.12
* sbt
* Linux (Debian 12 dans l'environnement de développement)

## Installation

1. Cloner le projet.
2. Déposer les fichiers CSV dans `resources/`.
3. Vérifier l'installation de Java, Spark et sbt.

## Compilation

```bash
sbt clean compile package
```

Le JAR est généré dans :

```text
target/scala-2.12/
```

## Exécution

### Bronze

```bash
spark-submit \
  --class BronzeLayer \
  --master local[*] \
  target/scala-2.12/projet.jar
```

### Silver

```bash
spark-submit \
  --class SilverLayer \
  --master local[*] \
  target/scala-2.12/projet.jar
```

### Gold

```bash
spark-submit \
  --class GoldLayer \
  --master local[*] \
  target/scala-2.12/projet.jar
```

## Résultat final

Le résultat est écrit dans :

```text
data/gold/patients_fhir/
```

Format :

```text
JSON Lines
```

Chaque ligne contient une ressource Patient FHIR R4 complète.

### Exemple

```json
{
  "resourceType": "Patient",
  "id": "800000123",
  "text": {
    "status": "generated",
    "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>"
  },
  "identifier": [
    {
      "use": "usual",
      "system": "http://example.org/fhir/identifier/ipp",
      "value": "800000123"
    }
  ],
  "active": true,
  "name": [
    {
      "use": "official",
      "family": "MARTIN",
      "given": [
        "Jean"
      ]
    }
  ],
  "gender": "male",
  "birthDate": "1985-03-12",
  "address": [
    {
      "line": [
        "12 RUE DE LA PAIX"
      ],
      "postalCode": "75002",
      "city": "PARIS",
      "country": "FRANCE",
      "use": "home"
    }
  ]
}
```

## Validation FHIR

Les ressources générées ont été validées avec HAPI FHIR R4.

Les contraintes suivantes sont respectées :

* `resourceType` obligatoire
* `id` présent
* `identifier` conforme
* `name` officiel présent
* `gender` conforme à FHIR
* `birthDate` normalisée
* `address` structurée
* `text.div` XHTML valide
* URLs d'extensions non réservées

Aucune erreur bloquante n'est remontée par le validateur.

## Couche Bronze

### Traitements réalisés

Lecture des CSV avec :

```scala
.option("header", true)
.option("multiLine", true)
.option("escape", "\"")
```

* Suppression des colonnes techniques `_c0`, `_c1`, etc.
* Écriture en Parquet.
* Conservation intégrale des données sources.

## Couche Silver

### Normalisation des dates

Transformation de plusieurs formats vers :

```text
yyyy-MM-dd
```

### Normalisation du sexe

| Valeur source | Valeur Silver |
| ------------- | ------------- |
| H             | H             |
| Homme         | H             |
| Male          | H             |
| F             | F             |
| Femme         | F             |
| Female        | F             |
| Autre         | null          |

### Prénoms

* Suppression des crochets et guillemets
* Découpage des prénoms multiples
* Capitalisation
* Déduplication

### Opposition

| Valeur source | Valeur Silver |
| ------------- | ------------- |
| O             | O             |
| Oui           | O             |
| true          | O             |
| 1             | O             |
| N             | N             |
| Non           | N             |
| false         | N             |
| 0             | N             |

## Couche Gold

### Résolution des IPP

La table `identifiants_ipp` constitue la référence principale.

Règles :

* utilisation de `ipp_principal` lorsqu'il existe ;
* sinon, conservation de l'IPP courant.

### Sélection du meilleur enregistrement

Priorités :

1. IPP actif ;
2. date de fin de validité la plus récente ;
3. valeurs nulles considérées comme `9999-12-31`.

### Adresses

* normalisation en majuscules ;
* suppression des doublons ;
* agrégation via `collect_set` ;
* conversion en tableaux FHIR.

### Opposition

La présence d'au moins un indicateur `O` rend le patient opposé.

### Construction FHIR

Les champs suivants sont produits :

* `id`
* `identifier`
* `active`
* `name`
* `gender`
* `birthDate`
* `address`
* `extension`
* `text`

## Hypothèses et arbitrages

### IPP principal

La table des identifiants IPP est considérée comme la source de vérité.

### Patients sans nom

Les patients sans `nom_naissance` sont exclus de la couche Gold.

### Dates

Toutes les dates sont converties en :

```text
yyyy-MM-dd
```

### Sexe inconnu

Les valeurs non reconnues deviennent :

```text
unknown
```

dans FHIR.

### Opposition absente

Une opposition absente est considérée comme négative.

## Anomalies détectées

| Anomalie                    | Traitement                 |
| --------------------------- | -------------------------- |
| Dates multiples             | Normalisation              |
| Sexe libre                  | Mapping H/F                |
| Prénoms complexes           | Nettoyage et déduplication |
| Adresses en double          | `collect_set`              |
| Type d'adresse manquant     | `home` par défaut          |
| IPP déprécié sans principal | auto-référence             |
| Opposition textuelle        | O/N                        |

## Pistes d'amélioration

* Ajout de tests unitaires Spark
* Contrôles qualité Bronze/Silver/Gold
* Validation FHIR automatisée dans le pipeline
* Ajout des téléphones et emails
* Gestion d'autres identifiants (NIR, INS, etc.)
* Externalisation des mappings dans des fichiers de configuration
* Optimisation des jointures Spark
* Déploiement via une API REST

## Personnalisation

Les mappings métiers sont centralisés dans :

* `SilverLayer.scala`
* `GoldLayer.scala`

Le format de sortie peut être changé facilement :

```scala
.write.json(...)
.write.csv(...)
.write.text(...)
```

## Licence

Projet fourni à des fins pédagogiques et d'évaluation technique.

Libre d'adaptation et de réutilisation.

**Auteur : Abdelhakim MENINA**

**Date : Juillet 2026**
