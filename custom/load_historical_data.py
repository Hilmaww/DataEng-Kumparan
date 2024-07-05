if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.decorators import loader
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader

@custom
def load_to_bigquery(df):
    table_id = 'your_project.your_dataset.articles'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    with BigQuery.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(df, table_id, if_exists='replace')

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
