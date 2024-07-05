if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.decorators import transformer
import pandas as pd
from collections import Counter

def word_count_dict(text):
    words = text.split()
    word_counts = Counter(words)
    return dict(word_counts)

@custom
def transform_data(df: pd.DataFrame):
    # Additional transformations
    df['word_count'] = df['content'].apply(lambda x: len(x.split()))
    df['title_length'] = df['title'].apply(lambda x: len(x))
    df['is_deleted'] = df['deleted_at'].notnull()

    # Extracting date parts for created_at
    df['created_year'] = df['created_at'].dt.year
    df['created_month'] = df['created_at'].dt.month
    df['created_day'] = df['created_at'].dt.day

    # Extracting date parts for updated_at
    df['updated_year'] = df['updated_at'].dt.year
    df['updated_month'] = df['updated_at'].dt.month
    df['updated_day'] = df['updated_at'].dt.day

    # Extracting date parts for published_at
    df['published_year'] = df['published_at'].dt.year
    df['published_month'] = df['published_at'].dt.month
    df['published_day'] = df['published_at'].dt.day

    # Word count transformation
    word_counts_list = []
    for idx, row in df.iterrows():
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
