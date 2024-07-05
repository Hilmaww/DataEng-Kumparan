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
    query = "SELECT * FROM articles WHERE updated_at > NOW() - INTERVAL '1 hour' OR deleted_at IS NOT NULL;"
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
