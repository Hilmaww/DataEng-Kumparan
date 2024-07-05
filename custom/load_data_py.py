if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.decorators import loader
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader

@custom
def load_to_bigquery(df):
    table_id = 'data-eng-428408.Kumparan.articles'
    articles_df, word_counts_df = df
    articles_table_id = 'data-eng-428408.Kumparan.articles'
    word_counts_table_id = 'data-eng-428408.Kumparan.word_counts'
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    with BigQuery.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Load articles
        loader.export(articles_df, articles_table_id)

        # Load word counts
        loader.export(word_counts_df, word_counts_table_id)
        
        # Handle deletions in BigQuery
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



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
