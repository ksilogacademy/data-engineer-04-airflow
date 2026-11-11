# Projet 4 — Pipeline avec Airflow : Consignes de clôture : 28 novembre 2026 à 23h59

## Contexte et objectif

Le Projet 4 commence exactement là où le Projet 3 s'arrête. Ton architecture cloud fonctionne : tu sais extraire un mois, le nettoyer, le convertir en Parquet, le déposer sur S3, le charger dans Snowflake et lancer dbt. Mais **c'est toi qui fais tourner chaque étape, à la main, dans le bon ordre**. Si tu oublies une commande, si une étape échoue à 3 h du matin ou si un collègue doit relancer février, personne ne sait ce qui s'est passé, ni où reprendre.

Un pipeline qui dépend d'un humain pour s'enchaîner n'est pas un pipeline de production. Dans ce projet, tu ne construis **aucune nouvelle source de données** et tu ne changes pas ton Data Warehouse : tu **orchestres** ce qui existe avec **Apache Airflow**. Airflow ne transforme pas tes données à ta place, il décide **quoi lancer, dans quel ordre, quand, combien de fois, et que faire quand ça casse**.

L'objectif n'est pas de « faire apparaître un DAG dans une interface ». C'est de savoir répondre à une question plus importante, et de pouvoir le prouver :

> **Comment passer d'une suite d'étapes que je lance moi-même à un pipeline planifié, rejouable mois par mois, qui s'arrête proprement quand il échoue, et dont je peux expliquer chaque exécution après coup ?**

Dans la série, ce projet correspond à l'étape « **Orchestration** » : les **sources** (Projet 1), la **transformation** (Projets 1 et 2) et le **chargement** (Projet 3) sont déjà là, Airflow vient les relier.

L'architecture attendue :

```text
        Airflow — planification mensuelle : 1 exécution (DAG run) = 1 mois
                                   │
                                   ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ Vérifications préalables : connexions, jeton API, données du │
   │ mois disponibles                                             │
   └───────────────────────────────┬──────────────────────────────┘
                                   ▼
   Extraction API (Projet 1) ──► Validation vs count(*) ──► Nettoyage + rapport qualité
                                   │
                                   ▼
   Conversion Parquet + upload S3 (Projet 3)        …/year=2023/month=MM/
                                   │
                                   ▼
   Chargement Bronze du mois : COPY INTO (Projet 3)
                                   │
                                   ▼
   dbt build : Silver → Gold, modèles incrémentaux (Projets 2 et 3)
                                   │
                                   ▼
   Contrôle de cohérence final  ──►  Rapport de run (JSON)
                                   │
        Échec à n'importe quelle étape :
        retries → alerte → les tâches suivantes ne s'exécutent pas
```

- **Dataset** : le même que les Projets 1, 2 et 3 — [NYC TLC Trip Record Data — Yellow Taxi (2023)](https://data.cityofnewyork.us/Transportation/2023-Yellow-Taxi-Trip-Data/4b4i-vvec)
- **Dictionnaire des champs** : [TLC Trip Record Data Dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- **Projets précédents** : [Projet 1 — Pipeline API](https://github.com/ksilogacademy/data-engineer-01-pipeline-api) · [Projet 2 — Data Warehouse](https://github.com/ksilogacademy/data-engineer-02-data-warehouse) · [Projet 3 — Pipeline Cloud](https://github.com/ksilogacademy/data-engineer-03-data-akehouse)

---

## 1. Prérequis

Avant de commencer, assure-toi d'avoir en main :

- **Les Projets 1, 2 et 3 terminés et fonctionnels** : tu vas réutiliser leur code, leurs scripts SQL, leur projet dbt et leur configuration AWS. Si l'un d'eux ne tourne plus, répare-le d'abord : Airflow ne corrige pas un pipeline cassé, il l'exécute fidèlement.
- **Les accès cloud du Projet 3 encore actifs** : bucket S3, rôle IAM, Storage Integration, compte Snowflake. Vérifie dès maintenant que ton essai Snowflake est toujours valide et que ton alerte de budget AWS existe toujours. Si ce n'est pas le cas, tu dois le savoir avant de te lancer, pas au milieu du projet.
- **Docker et Docker Compose v2** installés sur ta machine. Airflow ne s'installe pas nativement sous Windows : utilise Docker Desktop (avec WSL 2) ou un environnement Linux. Prévois **au moins 4 Go de mémoire** alloués à Docker (8 Go idéalement), sans quoi les conteneurs redémarrent en boucle.
- **Python** (versions supportées par ta version d'Airflow) et de bonnes bases en Python : fonctions, modules, gestion des erreurs, variables d'environnement.
- **Des bases Git/GitHub** : tu vas versionner ton travail proprement, en excluant tout ce qui ne doit jamais être public.

Tu dois aussi comprendre, avant de commencer ou en cours de projet, les notions suivantes : orchestrateur vs outil de transformation, DAG, tâche, opérateur, dépendance, graphe acyclique, planification (*schedule*), intervalle de données (*data interval*), date logique, `catchup`, backfill, idempotence, retry, timeout, *trigger rule*, XCom, Connection, Variable, provider, sensor, TaskGroup, pool, scheduler, worker, base de métadonnées, logs de tâche.

### Sources et documentation

Commence par la documentation officielle plutôt que par des tutoriels trouvés au hasard. **Utilise la documentation d'Airflow 3** : Airflow 2 n'est plus maintenu depuis avril 2026, et beaucoup de tutoriels en ligne décrivent encore l'ancienne version (imports, paramètres et comportements différents).

**Airflow**
- **Airflow** — documentation générale : [airflow.apache.org/docs](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- **Airflow** — installation avec Docker Compose : [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- **Airflow** — premier DAG : [Fundamentals](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html) · [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- **Airflow** — concepts : [Dags](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) · [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html) · [Sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) · [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html) · [Variables](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html)
- **Airflow** — connexions : [Managing Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) (voir la section sur les variables d'environnement `AIRFLOW_CONN_*`)
- **Airflow** — bonnes pratiques : [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- **Airflow** — journalisation, surveillance et callbacks : [Logging & Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/index.html)
- **Airflow** — providers : [Providers packages](https://airflow.apache.org/docs/apache-airflow-providers/index.html), notamment les providers Amazon et Snowflake.

**Rappels des projets précédents**
- **dbt** — [Incremental models](https://docs.getdbt.com/docs/build/incremental-models) · [Commands](https://docs.getdbt.com/reference/dbt-commands)
- **Snowflake** — [COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table) · [Warehouses overview](https://docs.snowflake.com/en/user-guide/warehouses-overview)
- **AWS** — [S3 User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) · [AWS Budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)

Si l'un de ces points n'est pas clair pour toi, c'est le moment de combler la lacune — pas après avoir écrit la moitié de ton DAG.

---

## 2. Consigne

À partir du pipeline des Projets 1 à 3, construis une orchestration Airflow qui réalise, dans l'ordre, les étapes suivantes :

1. **Environnement** : faire tourner **Airflow 3** en local avec Docker Compose, dans une image à toi, de façon reproductible, avec tes connexions et secrets injectés sans jamais être écrits dans le dépôt.
2. **Orchestration** : écrire un **DAG** qui enchaîne automatiquement, pour **un mois donné**, toutes les étapes du pipeline (extraction, validation, nettoyage, conversion Parquet, dépôt S3, chargement Bronze, dbt, contrôle final), avec des dépendances explicites et lisibles.
3. **Planification et rejeu** : planifier le DAG **mensuellement**, **rattraper** janvier, février et mars 2023 par un backfill, et pouvoir **rejouer un seul mois** sans toucher aux autres ni créer de doublon.
4. **Robustesse** : configurer retries et timeouts de façon réfléchie, garantir qu'une étape en échec **bloque** les suivantes, et **simuler des pannes** pour le prouver.
5. **Surveillance** : rendre chaque exécution **explicable après coup** : logs lisibles, rapport de run par mois, alerte en cas d'échec.

L'ensemble doit être reproductible par un tiers à partir de ton README, sans intervention cachée.

### Le besoin opérationnel

L'équipe d'analystes est contente de ses trois mois de données. Elle veut maintenant que **le pipeline tourne tout seul**, et que toi, responsable du pipeline, tu puisses répondre à des questions comme :

- **Automatisation** : quand un nouveau mois doit être traité, est-ce qu'il arrive jusqu'à Gold sans qu'aucun humain ne lance la moindre commande ?
- **Fiabilité** : si l'API est lente ou indisponible, est-ce que le pipeline réessaie, finit par alerter, et surtout ne charge pas un mois incomplet ?
- **Rejouabilité** : peux-tu rejouer mars sans toucher à janvier ni à février, et relancer une tâche échouée sans dupliquer quoi que ce soit ?
- **Traçabilité** : pour chaque mois traité, combien de lignes ont été extraites, retirées, chargées, en combien de temps, et quelle étape a échoué le cas échéant ?
- **Maîtrise** : le pipeline réveille-t-il ton warehouse Snowflake uniquement quand c'est nécessaire, et peut-on l'arrêter sans rien casser ?

**Ces questions ne veulent pas dire qu'il faut un DAG par question.** Comprends d'abord le besoin, puis choisis ton découpage en tâches.

---

## 3. Directives

### Les données et le code

Le Projet 4 réutilise **le pipeline, les scripts et le projet dbt de tes Projets 1 à 3**, et uniquement eux. Tu ne dois pas :

- télécharger un autre dataset, ni utiliser un fichier déjà préparé trouvé sur Internet ;
- réécrire ton Data Warehouse ou ta modélisation : ce n'est pas l'objet de ce projet ;
- contourner l'orchestration en lançant un script géant qui fait tout « en une seule tâche » : un DAG avec une seule tâche n'orchestre rien.

Les trois mois restent **janvier, février et mars 2023**. Le but est de pouvoir **les obtenir par l'orchestrateur**, pas à la main.

### Pièges à anticiper

Un DAG qui s'affiche en vert une fois sur ta machine ne prouve presque rien. Avant d'écrire la moindre tâche, pose-toi ces questions et vérifie les réponses toi-même :

- Ton DAG tourne : mais **pour quel mois** ? Quelle date reçoit un run planifié, un run de backfill, un run déclenché à la main depuis l'interface ? Comment ton code en déduit-il l'année et le mois à traiter, sans jamais les écrire en dur ?
- Un run planifié « pour janvier » démarre-t-il en janvier, ou à une autre date ? Dans Airflow 3, qu'est-ce qui change selon que tu écris ta planification sous forme de chaîne cron ou avec un *timetable* explicite, et qu'est-ce que ton DAG reçoit réellement comme intervalle ? **Lis la documentation et vérifie par un test, ne suppose pas.**
- Le comportement par défaut de `catchup` est-il le même dans Airflow 2 et Airflow 3 ? Que se passerait-il, côté coût et côté charge, si tu laissais Airflow rattraper tous les mois depuis une date de départ trop ancienne ?
- Tes tâches s'exécutent-elles forcément sur la même machine ? Un fichier écrit sur le disque par la tâche A est-il garanti d'exister quand la tâche B démarre ? Que fais-tu passer d'une tâche à l'autre : **les données elles-mêmes, ou une référence vers elles** ? Pourquoi un XCom n'est-il pas fait pour transporter un mois de trajets de taxi ?
- Les dépendances Python d'Airflow, de ses providers et de dbt tiennent-elles dans le même environnement sans conflit ? Comment les isoles-tu, et comment le prouves-tu ?
- Si tu relances **une tâche déjà réussie**, ou **un run entier**, que se passe-t-il dans S3, dans Snowflake et dans dbt ? Chaque tâche est-elle idempotente *prise isolément*, ou seulement quand tout est rejoué du début ?
- Que se passe-t-il quand plusieurs runs tournent en même temps sur la même API, le même bucket et le même warehouse ? Qu'est-ce qui limite la concurrence, et est-ce toi qui l'as décidé ?
- Que fait ton fichier de DAG **au moment où Airflow le lit** (c'est-à-dire en permanence, pas seulement quand il s'exécute) ? Qu'est-ce qui est dangereux d'écrire au niveau supérieur du fichier ?
- Un secret absent de Git peut-il fuiter autrement ? (logs de tâche, valeurs affichées dans l'interface, captures d'écran, variables d'environnement affichées dans un `print`.)

### Comprendre avant de construire

Avant d'ouvrir Airflow, réponds **par écrit** à ces questions :

- **Inventaire.** Liste toutes les commandes que tu lances à la main aujourd'hui pour produire un mois, de l'extraction à `dbt build`. Chaque ligne de cet inventaire doit retrouver sa place dans ton DAG, ou être justifiée comme *hors orchestration*.
- **Dépendances.** Quelle étape a besoin de quelle autre ? Lesquelles pourraient tourner en parallèle ? S'il n'y en a presque pas pour un seul mois, d'où vient le parallélisme possible dans ton pipeline ?
- **Grain d'un run.** Qu'est-ce qu'*une exécution* de ton DAG représente exactement ? Pourquoi ce grain permet-il de rejouer un mois sans toucher aux autres ?
- **Panne.** Si l'étape N échoue, que doit-il se passer pour l'étape N+1, pour les données déjà écrites, et pour la reprise ? Quelles étapes peuvent être relancées seules ?
- **Où s'exécute quoi.** Quels traitements sont lourds (extraction, nettoyage de plusieurs centaines de Mo, conversion Parquet) ? Doivent-ils s'exécuter dans le worker Airflow, dans un autre processus ou dans un service externe ? Pourquoi Airflow doit-il surtout **commander** le travail plutôt que le **faire** ?
- **Accès.** Qui a besoin d'accéder à quoi (Airflow vers l'API Socrata, S3, Snowflake, dbt), avec quels droits minimaux, et par quelles Connections ?

Cette analyse fait partie de tes livrables.

### Environnement Airflow

Fais tourner **Airflow 3** avec **Docker Compose**, en partant du fichier de la documentation officielle, mais **construis ta propre image** (`Dockerfile`) pour y installer tes dépendances et les providers nécessaires.

- **Version figée.** Indique explicitement la version d'Airflow de ton image : jamais `latest`. *Pourquoi une version non figée est-elle un problème de reproductibilité ?*
- **Simplification justifiée.** Le fichier officiel déploie plusieurs services (API server, scheduler, DAG processor, triggerer, worker, base de métadonnées, broker). Tu peux le simplifier si tu justifies ce que tu retires. *Quel est le rôle de chaque service que tu gardes ?*
- **Dépendances.** Les providers et bibliothèques de ton projet sont installés **dans l'image**, pas à la main dans un conteneur qui tourne. *Pourquoi ce qui est installé à la main disparaît-il au prochain redémarrage ?*
- **dbt.** Choisis et justifie comment dbt s'exécute dans ton environnement (voir le piège sur les dépendances).
- **Persistance.** Précise ce qui doit survivre à un `docker compose down` (logs, rapports de run, base de métadonnées) et ce qui peut être perdu.
- **Ressources.** Documente la mémoire nécessaire et la commande pour tout arrêter proprement.

L'interface d'Airflow est accessible en local : ne l'expose jamais sur Internet avec les identifiants par défaut de l'environnement d'exemple.

### Le DAG : orchestrer le pipeline d'un mois

Crée **un DAG** (ou plusieurs, si tu justifies ce découpage) qui orchestre le pipeline d'un mois. Les noms de tâches sont libres, mais **chaque tâche a une seule responsabilité** et un nom qui dit ce qu'elle fait. Il te faut au minimum des tâches pour :

- les **vérifications préalables** (connexions utilisables, jeton API présent, données du mois disponibles) ;
- l'**extraction** (Projet 1) et sa **validation** contre le `count(*)` officiel ;
- le **nettoyage** et la production du CSV et du rapport qualité (Projet 1) ;
- la **conversion Parquet** et l'**upload S3** (Projet 3) ;
- le **chargement Bronze** du mois (Projet 3) ;
- le **`dbt build`** (Projets 2 et 3) ;
- un **contrôle de cohérence final** : le nombre de lignes du mois dans S3, Bronze et Gold correspond-il au rapport qualité du Projet 1 ?
- la production d'un **rapport de run**.

Points d'attention :

- **Code métier hors du DAG.** Le fichier de DAG décrit *l'orchestration* (quoi, dans quel ordre, avec quels paramètres). La logique du Projet 1 et du Projet 3 vit dans du code importable, testable en dehors d'Airflow. *Comment ce code arrive-t-il dans ton conteneur : copie, sous-module Git, paquet installable ? Quel est le risque d'une copie qui diverge de l'original ?*
- **Dépendances explicites.** Le graphe doit se lire d'un coup d'œil dans l'interface. Utilise des **TaskGroups** là où ils clarifient (par exemple *extraction*, *chargement*, *qualité*). *Quelle est la différence entre une dépendance de données et une dépendance d'ordre ?*
- **Paramétrage.** Aucun mois, aucun chemin, aucun seuil écrit en dur dans le DAG. Les valeurs viennent de l'intervalle de données du run, de la configuration du projet ou de Variables. *Qu'est-ce qui mérite d'être une Variable Airflow, et qu'est-ce qui doit rester une constante de code ?*
- **TaskFlow ou opérateurs classiques ?** Choisis ce qui rend ton DAG le plus lisible et **explique** ton choix. *Quand utiliserais-tu un opérateur d'un provider (Snowflake, Amazon) plutôt que d'appeler une bibliothèque Python toi-même ?*
- **XCom.** Utilise-le pour faire passer de petites informations (un chemin, un nombre de lignes, un statut), jamais des données.
- **Sensor.** Si tu attends qu'une condition soit vraie (données du mois disponibles, fichier présent sur S3), utilise un *sensor* configuré correctement. *Pourquoi un sensor qui occupe un worker pendant des heures est-il un problème ? Qu'est-ce que le mode `reschedule` change ? Quel sensor pourrait, sans que tu le remarques, réveiller ton warehouse Snowflake et te coûter de l'argent ?* Si tu n'utilises aucun sensor, justifie pourquoi il n'est pas pertinent ici.
- **Concurrence.** Limite le nombre de runs simultanés (`max_active_runs`) et, si pertinent, utilise un **pool** pour protéger l'API ou le warehouse. Explique les valeurs choisies.

### Planification et rejeu

- **Planification mensuelle.** Le DAG est planifié une fois par mois. Définis des **bornes** (début et fin) pour qu'il ne rattrape que **janvier, février et mars 2023** : le dataset ne contient pas d'autres mois pertinents et chaque run coûte du calcul.
- **Backfill.** Lance un **backfill** pour obtenir les trois mois par l'orchestrateur, sans toucher au code entre deux mois. Les trois mois doivent aboutir au **même résultat** que ceux du Projet 3 (mêmes volumes par mois).
- **Rejeu d'un mois.** Montre comment rejouer **un seul mois** (par exemple février corrigé) sans modifier janvier ni mars. *Quelle différence fais-tu entre « relancer une tâche échouée », « relancer un run complet » et « rejouer un mois déjà réussi » ? Que doit garantir chacune de ces opérations ?*
- **Idempotence.** Reprends la stratégie du Projet 3 (rechargement d'un mois, protection contre le double chargement) et **prouve** qu'elle tient quand c'est Airflow qui rejoue : comptes de lignes avant/après, absence de doublon à tous les niveaux (S3, Bronze, Gold).

### Robustesse : quand ça casse

- **Retries et délais.** Configure `retries`, délai entre tentatives et, si utile, un délai croissant. Les valeurs dépendent de la **nature de la tâche** (un appel réseau n'a pas les mêmes besoins qu'un contrôle SQL). *Pourquoi relancer à l'identique une tâche qui échoue pour cause de données invalides ne sert-il à rien ?*
- **Timeouts.** Donne à chaque tâche un temps maximum cohérent. *Que se passe-t-il, sans timeout, quand une requête reste bloquée ?*
- **Arrêt en cascade.** Une tâche définitivement en échec doit empêcher les suivantes de tourner sur des données incomplètes. *Dans quels cas voudrais-tu au contraire qu'une tâche s'exécute malgré l'échec d'une autre (par exemple le rapport de run, ou un nettoyage) ? Quelle règle de déclenchement (*trigger rule*) utilises-tu pour cela ?*
- **Scénarios de panne.** Simule **au moins trois pannes** de natures différentes et documente, avec preuves, ce que fait ton pipeline : par exemple jeton API invalide ou API injoignable, fichier absent de S3, échec d'un test dbt, avertissement distinct d'une erreur bloquante. Pour chacune : ce qui a échoué, combien de tentatives, ce que les tâches suivantes ont fait, comment tu as repris, et ce que tu as vérifié ensuite (aucun doublon, aucun mois partiel).

### Surveillance, logs et alertes

Le jour où un mois pose problème, tu dois comprendre pourquoi **sans relancer quoi que ce soit**.

- **Logs de tâche.** Journalise, étape par étape, avec des messages qui permettent de comprendre un échec sans relire tout le code (comme au Projet 1). *Où Airflow range-t-il les logs, et que perds-tu si ce dossier n'est pas persistant ?*
- **Rapport de run.** À chaque exécution, produis un **rapport de run** (JSON, Markdown… libre) pour le mois traité : lignes extraites, retirées, chargées, durée de chaque grande étape, résultat du contrôle de cohérence, statut final. *Où est-il stocké pour survivre à la suppression de tes conteneurs ?*
- **Alerte d'échec.** Mets en place un *callback* d'échec qui signale clairement un problème (message structuré dans les logs, fichier d'alerte…). Une notification réelle (e-mail, messagerie) est un plus : si tu en ajoutes une, son secret vit dans une Connection, jamais dans le code.
- **Lecture de l'interface.** Tu dois savoir lire la vue Graphe et la vue Grille pour dire, en quelques secondes, quel mois a réussi, quelle tâche a échoué, combien de fois elle a été réessayée et pourquoi les tâches suivantes ne se sont pas exécutées.

### Tests

- **Intégrité des DAG.** Un test automatique vérifie que tes DAG se chargent **sans erreur d'import** et respectent tes règles (par exemple : chaque tâche a un timeout, aucun DAG sans propriétaire ni description). *Pourquoi détecter une erreur de syntaxe avant de déployer vaut-il mieux que la voir dans l'interface ?*
- **Tests du code métier.** Les fonctions importantes (calcul du mois, contrôles de cohérence, construction des chemins S3) ont des tests unitaires qui ne dépendent pas d'Airflow.
- **Test d'une tâche isolée.** Tu sais exécuter un DAG ou une tâche en local sans passer par le scheduler, et tu expliques quand c'est utile.

### Reproductibilité et secrets

Un autre développeur doit pouvoir, en suivant ton README : configurer AWS et Snowflake (renvoi vers tes projets précédents), construire l'image, démarrer Airflow, déclarer les connexions, lancer le backfill des trois mois, lire les résultats, lancer les tests, puis tout arrêter et nettoyer. Documente les commandes nécessaires.

**Aucun secret ne doit être présent dans Git** : clé d'accès AWS, mot de passe Snowflake, jeton Socrata, clé Fernet et clés secrètes d'Airflow, URI de connexion contenant un mot de passe, fichier `.env`, `profiles.yml` avec identifiants, ou tout fichier de configuration généré contenant l'un d'eux. Les Connections et Variables sensibles sont fournies par **variables d'environnement** (par exemple avec le préfixe `AIRFLOW_CONN_`) ou un autre mécanisme hors dépôt. Fournis un `.env.example` **sans valeurs réelles**, et fais en sorte que ton `.gitignore` empêche ces fichiers d'être versionnés. Une fuite dans l'historique Git reste une fuite même si le fichier est supprimé ensuite.

Pense aussi à ce que tu montres dans tes captures d'écran de l'interface Airflow : masque tes identifiants de compte AWS et Snowflake, et vérifie qu'aucune valeur sensible n'apparaît dans les logs ou les champs affichés.

### Maîtrise des ressources et des coûts

Airflow en local ne coûte pas de crédits, mais **ce qu'il déclenche en coûte**.

- **Snowflake** : seules les tâches qui en ont besoin utilisent le warehouse, qui se suspend automatiquement. *Quand ton warehouse consomme-t-il des crédits pendant un run Airflow, et quand n'en consomme-t-il pas ?*
- **Rattrapage** : un `catchup` ou un backfill mal borné peut lancer des dizaines de runs, donc des dizaines d'extractions et de chargements. Explique comment tu l'évites.
- **AWS** : ton alerte de budget du Projet 3 est toujours active, et rejouer un mois ne multiplie pas les fichiers inutiles dans le bucket.
- **Ta machine** : tu documentes comment arrêter Airflow (`docker compose down`) et libérer l'espace disque (volumes, images), ainsi qu'une section **« Nettoyage »** qui renvoie aussi à celle du Projet 3 pour les ressources cloud.

### Liberté d'architecture

L'arborescence proposée plus bas est un minimum, pas un carcan : elle peut évoluer si tu peux la justifier, à condition de rester **cohérente**, **documentée** dans ton `README.md` et **lisible à l'échelle du projet** (une bonne architecture te permet d'ajouter un nouveau DAG ou une nouvelle tâche sans tout réorganiser).

Les fonctionnalités d'Airflow non listées ici (planification par jeux de données, tâches dynamiques, tâches différables, etc.) restent à ta discrétion : si tu les utilises, explique le problème qu'elles résolvent ; si tu choisis de ne pas les utiliser, il n'est pas nécessaire de le justifier tant que ton DAG répond au besoin.

---

## 4. Livrable

### Ce que tu dois rendre

Un dépôt Git propre contenant au minimum :

```text
.
├── README.md
├── .gitignore
├── .env.example
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── dags/                   # DAG(s) : orchestration uniquement
├── include/                # code métier des Projets 1 et 3, importable par les DAG
├── dbt/                    # projet dbt (issu des Projets 2 et 3)
├── sql/
│   └── snowflake/          # scripts Snowflake (integration, stage externe, Bronze, contrôles)
├── aws/                    # policies IAM et trust policy (JSON, sans secret)
├── tests/                  # intégrité des DAG + tests unitaires
└── docs/
```

Les dossiers générés par Airflow (`logs/`, `config/`, `plugins/` selon ton montage) ne sont **pas versionnés**.

- **L'environnement Airflow** (`Dockerfile`, `docker-compose.yaml`, `requirements.txt`) qui démarre à partir du dépôt, avec une version d'Airflow figée.
- **Le ou les DAG** (dans `dags/`) et le **code métier** qu'ils appellent (dans `include/` ou équivalent).
- **Les tests** (dans `tests/`) : intégrité des DAG et tests unitaires.
- **Un `README.md`** qui explique : le contexte et l'objectif, l'architecture, les prérequis, l'installation, la configuration (image, Connections, Variables, secrets), le lancement d'Airflow, le backfill des trois mois, le rejeu d'un mois, la lecture de l'interface, les tests, le nettoyage, les choix importants (avec leur *pourquoi*) et les limites du projet (notamment ce qui changerait pour passer en production). Il doit aussi répondre aux questions « à résoudre » posées dans ces consignes.
- **Une documentation d'architecture** (dans `docs/`) : un schéma du **graphe du DAG** et un schéma de l'**environnement** (services Airflow, accès à Snowflake, S3 et API, où vivent les secrets), ainsi que ton analyse écrite (inventaire, dépendances, grain d'un run, pannes, où s'exécute quoi).
- **Des preuves** (dans `docs/`) permettant de vérifier : l'interface Airflow sans erreur d'import, la vue Graphe du DAG, la vue Grille avec les trois runs de janvier, février et mars, les logs d'une tâche, un rapport de run, le rejeu d'un mois (comptes avant/après), au moins trois scénarios de panne (échec, tentatives, arrêt en cascade, reprise), la cohérence des volumes par mois (S3, Bronze, Gold vs rapport qualité), les tests, et la suspension du warehouse. Vérifie que tes captures ne montrent aucun identifiant sensible.

### Barème de validation — /100 points

Ton projet sera noté selon le barème ci-dessous. Chaque ligne est vérifiée soit automatiquement (script), soit par une lecture humaine ou assistée (pour juger la qualité d'une justification, pas juste sa présence).

**Règle éliminatoire — prioritaire sur tout le reste :** si une clé d'accès AWS, un mot de passe, un jeton, une clé Fernet ou clé secrète Airflow, une URI de connexion avec mot de passe, une clé privée ou tout autre secret apparaît à un seul endroit de l'historique Git (même dans un commit ancien, même supprimé depuis), **le score est automatiquement 0/100**, quel que soit le reste du projet.

**Seuil de réussite : 70/100.** En dessous, le projet est « à revoir » ; à partir de 70, il est « validé ».

#### Bloc 1 — Environnement Airflow et sécurité (15 pts) · *vérification automatique*

| Critère | Points |
| --- | --- |
| Airflow 3 démarre avec `docker compose` à partir du dépôt, avec une image à soi et une version figée ; les DAG apparaissent sans erreur d'import | 5 |
| Connections et Variables sensibles fournies hors dépôt (variables d'environnement ou équivalent), `.env.example` sans valeur réelle, aucun secret dans le code, le `Dockerfile`, le Compose ou l'historique | 6 |
| Maîtrise des ressources et des coûts : concurrence limitée, rattrapage borné, warehouse à suspension automatique, procédure de nettoyage documentée | 4 |

#### Bloc 2 — DAG, dépendances et planification (25 pts) · *mixte*

| Critère | Points | Vérification |
| --- | --- | --- |
| Test d'intégrité des DAG présent et qui passe (chargement sans erreur, règles de base respectées) | 5 | Auto |
| Pipeline complet orchestré : tâches à responsabilité unique, dépendances explicites, graphe lisible (TaskGroups pertinents), code métier séparé du fichier de DAG | 8 | Mixte |
| Planification mensuelle bornée, mois déduit de l'intervalle du run (aucune date en dur), backfill de janvier à mars 2023 réussi avec des volumes cohérents avec le Projet 3 | 8 | Auto |
| Fonctionnalités d'Airflow utilisées à bon escient (TaskFlow ou opérateurs, XCom léger, sensor, pools, règles de déclenchement) et justifiées | 4 | Humaine / assistée |

#### Bloc 3 — Robustesse, idempotence et surveillance (20 pts) · *mixte*

| Critère | Points | Vérification |
| --- | --- | --- |
| Retries, délais et timeouts configurés tâche par tâche selon leur nature, pas une valeur unique appliquée sans réflexion | 5 | Auto |
| Une tâche en échec bloque les suivantes ; relancer une tâche ou rejouer un mois ne duplique rien (S3, Bronze, Gold), avec preuves avant/après | 7 | Mixte |
| Surveillance : logs exploitables, rapport de run par mois, callback d'échec | 5 | Mixte |
| Au moins trois scénarios de panne simulés, documentés avec preuves et reprise vérifiée | 3 | Humaine / assistée |

#### Bloc 4 — Documentation et présentation (40 pts) · *mixte*

| Critère | Points | Vérification |
| --- | --- | --- |
| `README.md` complet et structuré (objectif, architecture, installation, configuration, lancement, backfill et rejeu, tests, nettoyage, limites) | 10 | Auto (présence des sections) |
| Documentation d'architecture et preuves : graphe du DAG, schéma de l'environnement, vues Airflow, logs, rapport de run, volumes par mois | 10 | Mixte |
| Les choix (découpage en tâches, grain d'un run, retries et timeouts, stratégie d'idempotence, gestion des secrets, isolation de dbt, concurrence, maîtrise des coûts) sont expliqués et argumentés dans le README, pas seulement mentionnés | 15 | Humaine / assistée |
| Dépôt propre et preuves présentes, sans secret, sans log ni fichier généré par Airflow versionné par erreur | 5 | Auto |

**Exemple concret pour le critère « choix » :** lister n'est pas argumenter.

- ❌ *Listé* : « J'ai mis 3 retries sur la tâche d'extraction. »
- ✅ *Argumenté* : « J'ai mis 3 retries avec un délai croissant sur l'extraction parce que l'API Socrata limite parfois le débit et qu'une nouvelle tentative quelques minutes plus tard réussit presque toujours ; en revanche, la tâche de contrôle de cohérence n'a aucun retry, car si les comptes de lignes ne correspondent pas, réessayer ne changera pas le résultat. »

Même chose pour chaque tâche, paramètre de planification, sensor, pool, Connection ou choix d'isolation de dbt : le README doit dire *pourquoi* ils existent. Et si tu décides de ne pas utiliser une fonctionnalité, justifie pourquoi elle n'est pas pertinente.

---

## 5. Soumission

**📅 Date limite : samedi 28 novembre 2026, 23h59 (heure de Dakar).**

Une fois ton projet terminé et poussé sur GitHub (dépôt public, sinon je ne peux pas y accéder) :

- **Commente le post LinkedIn du projet** avec le lien direct vers ton dépôt GitHub.
- Vérifie avant de poster que le lien est bien public (teste-le en navigation privée) et qu'il pointe vers la racine du dépôt, pas vers un fichier ou une branche spécifique.
- Un seul commentaire par apprenant suffit ; si tu push des corrections après coup, un commentaire de mise à jour avec « edit » en préfixe évite la confusion sur quelle version évaluer.

---

## Checklist finale avant de considérer le projet terminé

- [ ] Airflow 3 démarre avec `docker compose` à partir du dépôt, dans une image à toi avec une version figée, et les DAG n'ont aucune erreur d'import.
- [ ] Le DAG orchestre tout le pipeline d'un mois (extraction, validation, nettoyage, Parquet, S3, Bronze, dbt, contrôle final, rapport) avec des dépendances explicites et lisibles.
- [ ] Aucun mois, chemin ou seuil n'est écrit en dur : le mois vient de l'intervalle du run.
- [ ] Le backfill de janvier, février et mars 2023 réussit, avec des volumes par mois cohérents avec le Projet 3.
- [ ] Rejouer une tâche, un run ou un mois ne crée aucun doublon (S3, Bronze, Gold), preuves à l'appui.
- [ ] Retries et timeouts sont configurés selon la nature de chaque tâche, et une tâche en échec bloque les suivantes.
- [ ] Au moins trois scénarios de panne ont été simulés, documentés et la reprise vérifiée.
- [ ] Chaque exécution produit des logs exploitables, un rapport de run et, en cas d'échec, une alerte.
- [ ] Le test d'intégrité des DAG et les tests unitaires passent.
- [ ] La concurrence et le rattrapage sont bornés, le warehouse se suspend automatiquement, l'alerte de budget AWS existe toujours, et le README explique comment tout arrêter et nettoyer.
- [ ] Aucun secret (clé AWS, mot de passe, jeton, clé Fernet, URI de connexion, `.env`, `profiles.yml` avec identifiants) n'est versionné ni présent dans l'historique Git, et aucune capture ni log ne les montre.
- [ ] `README.md` est à jour, permet une prise en main autonome, et argumente les choix au lieu de les lister.
