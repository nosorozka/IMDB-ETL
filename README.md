# **ETL proces datasetu IMDB**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **IMDB** datasetu. Cieľom projektu je uviesť do praxe poznatky o transformáciách ETL, preskúmavať správanie používateľov a ich preferencie publika na základe hodnotení filmov a pokladničných dokladov. Výsledný dátový model umožní multidimenzionálnu analýzu a vizualizáciu vyššie uvedených ukazovateľov.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov a ich hodnotení, režisérov a zamestnancov filmových štúdií. Táto analýza umožňuje identifikovať trendy v preferenciách publika a najpopulárnejších filmoch.

Zdrojové dáta pochádzajú z сvičnej databázy: Filmová produkcia pre SQL príkazy - IMDB (MySQL) k dispozícii [tu](https://edu.ukf.sk/mod/folder/view.php?id=252868). Dataset obsahuje štyri hlavné tabuľky:
- `movie`
- `ratings`
- `ganre`
- `names`

a dve spojovacie tabuľky:

- `director_mapping`
- `role_mapping`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/nosorozka/IMDB-ETL/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%20(446).png">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDB</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **Model snehovej vločky (Snowflake schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_movie`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_names`**: Obsahuje podrobné informácie o a zamestnancoch filmových štúdií a režiséroch (meno, dátum narodenia, pohlavie a ich úlohy vo filme).
- **`dim_ratings`**: Zahrňuje informácie o hodnotení filmov (priemerné hodnotenie filmu, počet hodnotení a median hodnotení).                            Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, pohlavie, povolanie a vzdelanie.
- **`dim_genre`**: Obsahuje filmový žáner.
- **`dim_languages`**: Obsahuje jazyky, v ktorých bol film uvedený
- **`country_dim`**: Zahŕňa krajiny, v ktorých bol film natočený

Štruktúra snehovej vločky je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/nosorozka/IMDB-ETL/blob/main/%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%20(445).png">
  <br>
  <em>Obrázok 2 Snowflake schema pre IMDB</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Najprv importujem IMDB_MySql.SQL skript do phpmyadmin na extrahovanie súborov csv s tabuľkami, ktoré potrebujeme. To je potrebné urobiť, v našom konkrétnom prípade to nebude trvať veľa času, ale pri škálovaní takýchto transformácií je lepšie tento prístup nepoužívať, pretože v skutočnosti načítame lokálny server, je lepšie okamžite mať prístup k potrebným tabuľkám priamo.

Dáta zo zdrojového datasetu (formát `.csv`) boli nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE my_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, zamestnancov filmových štúdií , recenziách a žánroch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO rating_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

jedna z tabuliek obsahuje hodnotu prázdne bunky. Aby sme sa vyhli chybám, používame vlastný formát nahrávania, ktorý nahradí slovo "NULL" alebo prázdny reťazec hodnotou Null v Snowflake.

```sql
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', '');
```

---
### **3.1 Transfor (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Transformácia zahŕňala oddelenie stĺpcov "country" a "languages" do samostatných tabuliek. Tento proces predstavuje normalizáciu údajov, ktorá zlepšuje ich štruktúru, umožňuje efektívnejšie spracovanie, jednoduchšiu obnovu a vizualizáciu informácií týkajúcich sa uvedených stĺpcov.
```sql
-- dim_languages
CREATE OR REPLACE TABLE dim_languages AS
SELECT DISTINCT
    TRIM(value) AS language_name
FROM movie_staging,
LATERAL FLATTEN(INPUT => SPLIT(languages, ',')) AS language_split;

-- dim_country
CREATE OR REPLACE TABLE dim_country AS
SELECT DISTINCT
    TRIM(value) AS country_name
FROM movie_staging,
LATERAL FLATTEN(INPUT => SPLIT(country, ',')) AS country_split;
```
Dimenzia dim_names bola navrhnutá na uchovávanie informácií o jednotlivcoch spojených s filmami. Obsahuje atribúty, ako sú meno, výška, dátum narodenia, známe filmy a odvodené údaje, ako sú rola (herec, herečka alebo neznáma) a pohlavie (mužské, ženské alebo neznáme). 

Tieto odvodené údaje sú vytvorené na základe kategórie definovanej v zdrojovej tabuľke role_mapping_staging. Táto dimenzia umožňuje vykonávať analýzy na základe charakteristík jednotlivcov a poskytuje obohatené údaje o pohlaví a roliach. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0, pretože uchováva nemenné údaje.
```sql
-- dim_names
CREATE OR REPLACE TABLE dim_names AS
SELECT 
    n.id,
    n.name,
    n.height,
    n.date_of_birth,
    n.known_for_movies,
    CASE
        WHEN r.category = 'actor' THEN 'actor'
        WHEN r.category = 'actress' THEN 'actress'
        ELSE 'unknown'
    END AS role,
    CASE
        WHEN r.category = 'actor' THEN 'male'
        WHEN r.category = 'actress' THEN 'female'
        ELSE 'unknown' 
    END AS sex
FROM names_staging n
JOIN role_mapping_staging r ON n.id = r.name_id;
```
Podobne `dim_genre` a `dim_ratings` obsahuju údaje ako sú názov žánru, priemerné hodnotenie, počet recenzií a median hodnotení. Táto dimenzia je typu SCD Typ 0, pretože údaje o žánroch a recenziách sa považujú za nemenné, napríklad priemerné hodnotenie a názvy žánrov sa nemenia.
očnosti.

Tabuľka `FACT_MOVIE` je pripojená ku všetkým dimenziám, aj prostredníctvom prepojovacích tabuliek. Zahŕňa kľúčové ukazovatele, ako je názov filmu, trvanie a produkčná spoločnosť.

```sql
CREATE TABLE fact_ratings AS
SELECT 
    r.ratingId AS fact_ratingID,
    r.timestamp AS timestamp,
    r.rating,
    d.dim_dateID AS dateID,
    t.dim_timeID AS timeID,
    b.dim_bookId AS bookID,
    u.dim_userId AS userID
FROM ratings_staging r
JOIN dim_date d ON CAST(r.timestamp AS DATE) = d.date
JOIN dim_time t ON r.timestamp = t.timestamp
JOIN dim_books b ON r.ISBN = b.dim_bookId
JOIN dim_users u ON r.userId = u.dim_userId;
```
Tabuľky `movie_language_mapping` a `movie_country_mapping` boli vytvorené pre mapovanie viacerých hodnôt (jazyky a krajiny) z jedného atribútu v tabuľke `movie_staging` do samostatných riadkov. Proces využíva funkciu LATERAL FLATTEN, aby umožnil oddeliť viacnásobné hodnoty uložené ako reťazce (napr. "English, French") a transformovať ich do štruktúry vhodnej na analýzu. Tieto tabuľky predstavujú normalizáciu údajov a uľahčujú agregáciu a vizualizáciu jazykových a geografických údajov spojených s filmami.

```sql
-- movie_language_mapping
CREATE OR REPLACE TABLE movie_language_mapping AS 
SELECT 
    m.id AS movie_id,
    TRIM(f.value::STRING) AS language_name
FROM movie_staging m,
     LATERAL FLATTEN(input => SPLIT(m.languages, ',')) f;
```

Tabuľka `movie_names_mapping` bola navrhnutá na mapovanie vzťahu medzi jednotlivcami (names_id) a filmami (movie_id). Obsahuje aj atribút director, ktorý rozlišuje, či daný jednotlivec vystupuje ako režisér filmu. Údaje sú napĺňané kombináciou vkladania údajov zo staging tabuliek `director_mapping_staging` a `role_mapping_staging`. Táto tabuľka umožňuje flexibilné analýzy vzťahov medzi jednotlivcami a ich úlohami vo filmoch.

```sql
CREATE OR REPLACE TABLE movie_names_mapping (
    names_id VARCHAR(10),     
    movie_id VARCHAR(10),     
    director BOOLEAN  
);

INSERT INTO movie_names_mapping (names_id, movie_id, director)
SELECT 
    name_id AS names_id, 
    movie_id,
    TRUE AS director 
FROM director_mapping_staging;

INSERT INTO movie_names_mapping (names_id, movie_id, director)
SELECT 
    name_id AS names_id, 
    movie_id,
    FALSE AS director 
FROM role_mapping_staging;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS rating_staging;
DROP TABLE IF EXISTS director_mapping_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu snehova vločka. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analyzovať preferencie publika a distribúciu vo filmovom priemysle, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa kníh, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/nosorozka/IMDB-ETL/blob/main/amazonbooks_dashboard.png">
  <br>
  <em>Obrázok 3 Dashboard IMDB datasetu</em>
</p>

---
### **Graf 1: Najpopulárnejšie dabingové jazyky (Top 10 kníh)**
Táto vizualizácia zobrazuje 10 jazykov, v ktorých sa filmy vydávajú alebo dabujú. To vám umožní identifikovať najobľúbenejšie jazyky medzi používateľmi. Napríklad sme zistili, že francúzština a španielčina patria medzi top 3. Informácie môžu byť užitočné pre marketingové kampane na propagáciu a zvýšenie ziskov
```sql
SELECT language_name, COUNT(*) AS count FROM movie_language_mapping
GROUP BY language_name
ORDER BY count DESC
LIMIT 10; 
```
---
### **Graf 2: 5 najobľúbenejších filmov podľa celkových zbierok**
Graf ukazuje, že 3 filmy v najlepších svetových zbierkach patria do tej istej franšízy, čo naznačuje, že pre korporácie je prospešné podporovať existujúce franšízy a originálne nápady neprinášajú veľký zisk.

```sql
SELECT DISTINCT title, CAST(REGEXP_REPLACE(WORLDWIDE_GROSS_INCOME, '[^0-9]', '') AS DECIMAL) AS worldwide_gross_income_numeric
FROM FAKT_MOVIE
WHERE WORLDWIDE_GROSS_INCOME != 'NULL' 
ORDER BY worldwide_gross_income_numeric DESC
LIMIT 5;
```
---
### **Graf 3: 5 najobľúbenejších filmov podľa priemerného hodnotenia**
Na grafe vidíme 3 filmy natočené v Indii, ktoré môžu naznačovať vývoj filmového priemyslu v tejto krajine a potenciálne sa môžu stať dôvodom investícií do Bollywoodu alebo propagáciu tovaru a služieb v tomto regióne.

```sql
SELECT rating::DECIMAL(10,2) AS rating, title FROM FAKT_MOVIE
WHERE WORLDWIDE_GROSS_INCOME != 'NULL'
ORDER BY rating DESC
LIMIT 5;

```
---
### **Graf 4: či závisí priemerné hodnotenie od dĺžky filmu**
Na grafe vidíme, že so zvyšujúcou sa dĺžkou filmu sa zvyšuje aj jeho skóre. Trend je jasne viditeľný až do aktivity 130 minút, čo môže naznačovať optimálny čas, ktorý je používateľ ochotný stráviť filmom.

```sql
SELECT DURATION, COUNT(*), round(avg(rating),2) FROM FAKT_MOVIE
GROUP BY DURATION
ORDER BY DURATION ASC;
```
---
### **Graf 5: Pomer mužov a žien vo filmovom priemysle**
Graf ukazuje rozdiely v počte hercov medzi mužmi a ženami. Údaje ukazujú, že v priemysle je viac mužov ako žien, čo môže určiť, že pre mužské pohlavie je o niečo jednoduchšie zamestnať sa vo filmovom priemysle 

```sql
SELECT sex, COUNT(*)FROM dim_names WHERE sex != 'unknown'
GROUP BY sex;
```
---
### **Graf 7: 10 krajin ktoré sa najčastejšie podieľajú na natáčaní filmov**

```sql
SELECT country_name, COUNT(*) AS count FROM movie_country_mapping
GROUP BY country_name
ORDER BY count DESC
LIMIT 10; 
```

Dashboard poskytuje úplný prehľad o údajoch a zároveň odpovedá na dôležité otázky týkajúce sa preferencií a správania používateľov. Vizualizácia uľahčuje interpretáciu údajov a dá sa použiť na optimalizáciu odporúčacích systémov, marketingových stratégií a knižničných služieb.

---

**Autor:** Sheiko Yehor
