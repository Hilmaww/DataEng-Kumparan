# ETL Pipeline Documentation

## 1. Background Problem Statement and Objectives

### Problem Statement
kumparan, an online media company, has been experiencing exponential growth in both user engagement and content production since its inception in 2016. To harness the power of this data and make informed business decisions, there is a need to develop a robust ETL (Extract, Transform, Load) pipeline that can handle the massive influx of data, ensure data consistency, and enable comprehensive analytics.

### Objectives
- **Extract** data from a PostgreSQL source database.
- **Transform** the data to meet analytical needs, including calculating word counts and creating derived metrics.
- **Load** the transformed data into Google BigQuery for scalable and efficient querying.
- Ensure the ETL pipeline runs hourly to keep the data up-to-date.

### Bonus Challenges
1. Considerations for handling historical data dating back to 2016.
2. Synchronizing data between the source and the data warehouse in the event of hard deletes in the source database.

## 2. Translating Objectives into Business Needs

### Business Needs
To provide insightful analytics and optimize content strategies, the following business needs were identified:
- **Content Performance Analysis**: Understand how content is performing in terms of views, engagement, and SEO metrics.
- **Content Optimization**: Optimize titles, content length, and publication times to maximize user engagement.
- **Predictive and Prescriptive Analytics**: Use historical data to forecast trends and provide actionable recommendations.

### Data Needs
To meet these business needs, the following data points are essential:
- Article metadata (ID, title, content, publication dates, author information)
- Derived metrics such as word counts and title lengths
- Information about deleted articles to maintain data consistency

## 3. Tech Stack

### Technologies Used
- **Google Cloud EC Instance**: Hosting and running the ETL pipeline.
- **Mage AI**: Orchestrating the ETL process.
- **Python**: Scripting the ETL steps.
- **Docker**: Containerizing the ETL application for portability and scalability.
- **PostgreSQL**: Source database for extracting data.
- **Google BigQuery**: Target data warehouse for storing and analyzing transformed data.

## 4. Database Structures

### Source Database Structure (PostgreSQL)
```sql
CREATE TABLE articles (
  id SERIAL PRIMARY KEY,
  title TEXT,
  content TEXT,
  published_at TIMESTAMP,
  author_id INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP
);
```

### BigQuery Tables Structures

#### Articles Table
```sql
CREATE TABLE articles (
  id INT64,
  title STRING,
  content STRING,
  published_at TIMESTAMP,
  author_id INT64,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP,
  word_count INT64,
  title_length INT64,
  is_deleted BOOLEAN,
  created_year INT64,
  created_month INT64,
  created_day INT64,
  updated_year INT64,
  updated_month INT64,
  updated_day INT64,
  published_year INT64,
  published_month INT64,
  published_day INT64
);
```

#### Word Counts Table
```sql
CREATE TABLE word_counts (
  article_id INT64,
  word STRING,
  count INT64
);
```

## 5. Architecture of the ETL

### Architecture Overview
The ETL pipeline is designed to run hourly, extracting data from PostgreSQL, transforming it into a suitable format, and loading it into BigQuery. The architecture includes:
- **Extraction Layer**: Extracts raw data from PostgreSQL.
- **Transformation Layer**: Applies transformations, calculates derived metrics, and prepares data for loading.
- **Loading Layer**: Loads the transformed data into BigQuery.

### Diagram
```plaintext
      +-------------------+         +-------------------+
      |                   |         |                   |
      |   PostgreSQL DB   |         |  Google BigQuery  |
      |                   |         |                   |
      +-------------------+         +-------------------+
               |                            |
               |                            |
               v                            v
        +-------------+               +-------------+
        |  Extract    |               |  Load       |
        |  from       |               |  to         |
        |  PostgreSQL |               |  BigQuery   |
        +-------------+               +-------------+
               |                            |
               |                            |
               v                            v
        +----------------------------------------+
        |                Mage Pipeline           |
        +----------------------------------------+
                          |
                          |
                          v
                    +-------------+
                    |  Schedule   |
                    |  Every Hour |
                    +-------------+
```

## 6. ETL Pipeline

### ETL Process
1. **Extract Data**: Extracts data from PostgreSQL, including recently updated articles and deleted articles.
2. **Transform Data**: Applies transformations such as calculating word counts, title lengths, and breaking down timestamps into components. Handles deletions by marking records.
3. **Load Data**: Loads the transformed data into BigQuery, ensuring data consistency and handling deletions.

## 7. Code

### Extract Data
#### Extract Data from PostgreSQL
```python
from mage_ai.data_preparation.decorators import extractor
import pandas as pd
from mage_ai.io.postgres import Postgres

@extractor
def extract_from_postgres():
    query = """
        SELECT * FROM articles WHERE updated_at > NOW() - INTERVAL '1 hour'
        UNION ALL
        SELECT id, NULL AS title, NULL AS content, NULL AS published_at, NULL AS author_id,
               NULL AS created_at, NULL AS updated_at, deleted_at
        FROM deleted_articles_log WHERE deleted_at > NOW() - INTERVAL '1 hour';
    """
    with Postgres.with_config('config/connections/postgres.yaml') as loader:
        df = loader.load(query)
    return df
```

### Transform Data
#### Transform Data and Calculate Derived Metrics
```python
from mage_ai.data_preparation.decorators import transformer
import pandas as pd
from collections import Counter

def word_count_dict(text):
    if not text:
        return {}
    words = text.split()
    word_counts = Counter(words)
    return dict(word_counts)

@transformer
def transform_data(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    df['is_deleted'] = df['deleted_at'].notnull()
    
    df['word_count'] = df.apply(lambda row: len(row['content'].split()) if not row['is_deleted'] else 0, axis=1)
    df['title_length'] = df.apply(lambda row: len(row['title']) if not row['is_deleted'] else 0, axis=1)

    df['created_year'] = df['created_at'].dt.year
    df['created_month'] = df['created_at'].dt.month
    df['created_day'] = df['created_at'].dt.day

    df['updated_year'] = df['updated_at'].dt.year
    df['updated_month'] = df['updated_at'].dt.month
    df['updated_day'] = df['updated_at'].dt.day

    df['published_year'] = df['published_at'].dt.year
    df['published_month'] = df['published_at'].dt.month
    df['published_day'] = df['published_at'].dt.day

    word_counts_list = []
    for idx, row in df.iterrows():
        if not row['is_deleted']:
            word_counts = word_count_dict(row['content'])
            for word, count in word_counts.items():
                word_counts_list.append({
                    'article_id': row['id'],
                    'word': word,
                    'count': count
                })

    word_counts_df = pd.DataFrame(word_counts_list)

    return df, word_counts_df
```

### Load Data
#### Load Data into BigQuery
```python
from mage_ai.data_preparation.decorators import loader
from mage_ai.io.bigquery import BigQuery

@loader
def load_to_bigquery(data):
    articles_df, word_counts_df = data
    articles_table_id = 'your_project.your_dataset.articles'
    word_counts_table_id = 'your_project.your_dataset.word_counts'
    
    with BigQuery.with_config('config/connections/bigquery.yaml') as loader:
        loader.export(articles_df, articles_table_id)
        loader.export(word_counts_df, word_counts_table_id)
        
        deleted_ids = articles_df[articles_df['is_deleted']]['id'].tolist()
        if deleted_ids:
            delete_query = f"""
                DELETE FROM `{articles_table_id}`
                WHERE id IN UNNEST({deleted_ids})
            """
            loader.execute(delete_query)

            delete_word_counts_query = f"""
                DELETE FROM `{word_counts_table_id}`
                WHERE article_id IN UNNEST({deleted_ids})
            """
            loader.execute(delete_word_counts_query)
```

### ETL Pipeline Definition
#### Mage Pipeline Definition
```yaml
name: etl_incremental
schedule:
  interval: '@hourly'
tasks:
  - name: extract_from_postgres
    function: extract_data.extract_from_postgres
  - name: transform_data
    function: transform_data.transform_data
    upstream_tasks: [extract_from_postgres]
  - name: load_to_bigquery
    function: load_data.load_to_bigquery
    upstream_tasks: [transform_data]
```

## 8. Bonus Challenges

### Handling Historical Data
To address the challenge of historical data dating back to 2016:
- Implement an initial bulk load process to load all historical data before starting the incremental ETL process.
- This involves creating a separate pipeline to handle the initial data load.

### Handling Hard Deletes
To handle hard deletes where rows are permanently removed from the source database:
- Use a deletion log table to track deleted articles.
-

 Modify the ETL pipeline to account for deletions by extracting data from the deletion log and ensuring these deletions are reflected in BigQuery.

#### Historical Data Pipeline
```yaml
name: etl_historical
tasks:
  - name: extract_historical_data
    function: extract_historical_data.extract_historical_data
  - name: transform_data
    function: transform_data.transform_data
    upstream_tasks: [extract_historical_data]
  - name: load_to_bigquery
    function: load_data.load_to_bigquery
    upstream_tasks: [transform_data]
```

#### Code for Historical Data Extraction
```python
from mage_ai.data_preparation.decorators import extractor
import pandas as pd
from mage_ai.io.postgres import Postgres

@extractor
def extract_historical_data():
    query = "SELECT * FROM articles;"
    with Postgres.with_config('config/connections/postgres.yaml') as loader:
        df = loader.load(query)
    return df
```
