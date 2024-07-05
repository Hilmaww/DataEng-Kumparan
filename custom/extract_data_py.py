if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.decorators import extractor
import pandas as pd
from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader

@custom
def extract_from_postgres():
    # Modify the extraction and loading steps to handle deletions.
    query = """
        SELECT * FROM articles WHERE updated_at > NOW() - INTERVAL '1 hour'
        UNION ALL
        SELECT id, NULL AS title, NULL AS content, NULL AS published_at, NULL AS author_id,
               NULL AS created_at, NULL AS updated_at, deleted_at
        FROM deleted_articles_log WHERE deleted_at > NOW() - INTERVAL '1 hour';
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        df = loader.load(query)
    return df



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
