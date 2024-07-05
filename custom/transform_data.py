if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

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
    # Handle deletions
    df['is_deleted'] = df['deleted_at'].notnull()
    
    # Only calculate word count for non-deleted rows
    df['word_count'] = df.apply(lambda row: len(row['content'].split()) if not row['is_deleted'] else 0, axis=1)
    df['title_length'] = df.apply(lambda row: len(row['title']) if not row['is_deleted'] else 0, axis=1)

    # Extracting date parts
    df['created_year'] = df['created_at'].dt.year
    df['created_month'] = df['created_at'].dt.month
    df['created_day'] = df['created_at'].dt.day

    df['updated_year'] = df['updated_at'].dt.year
    df['updated_month'] = df['updated_at'].dt.month
    df['updated_day'] = df['updated_at'].dt.day

    df['published_year'] = df['published_at'].dt.year
    df['published_month'] = df['published_at'].dt.month
    df['published_day'] = df['published_at'].dt.day

    # Word count transformation
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



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
