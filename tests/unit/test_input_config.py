import pytest

from dug_helpers.dug_utils import validate_datasource_config
from roger.Config import DatasourceConfig, DugInputsConfig
from roger.core import Util


@pytest.fixture()
def metadata():
    metadata = Util.read_relative_object("../metadata.yaml")
    return metadata


@pytest.mark.parametrize(
    "name,version",
    [
        ("topmed", "v1.0"),
        (" topmed ", " v1.0 "),
        (" topmed ", "v2.0"),
        ("topmed", "v2.0"),
        ("bdc", "v1.0"),
        (" bdc ", " v1.0 "),
        ("nida", "v1.0"),
        (" nida ", " v1.0 "),
    ]
)
def test_input_config_init(name, version):
    config = DatasourceConfig(name, version)
    assert config.name == name.strip()
    assert " " not in config.name

    assert config.version == version.strip()
    assert " " not in config.version


def test_dug_input_configs():
    config_dict = {
        'data_sets': [
            {
                'name': 'topmed',
                'version': 'v2.0',
            },
            {
                'name': 'bdc',
                'version': 'v1.0',
            },
        ]
    }

    config = DugInputsConfig.from_dict(config_dict)
    assert len(config.data_sets) == 2
    assert DatasourceConfig('topmed', 'v2.0') in config.data_sets
    assert DatasourceConfig('bdc', 'v1.0') in config.data_sets


def test_dug_input_configs_empty():
    config_dict = {}
    config = DugInputsConfig.from_dict(config_dict)
    assert len(config.data_sets) == 0


def test_validate_datasource_config(metadata):
    expected_data_format = "topmed"

    datasource_config = DatasourceConfig(
        name="topmed",
        version="v2.0"
    )
    assert validate_datasource_config(datasource_config, metadata, expected_data_format) == [
        "topmed_variables_v2.0.csv",
        "topmed_tags_v2.0.json"
    ]

    expected_data_format = "dbGaP"
    datasource_config = DatasourceConfig(
        name="bdc",
        version="v1.0"
    )
    assert validate_datasource_config(datasource_config, metadata, expected_data_format) == [
        "bdc_dbgap_data_dicts.tar.gz",
    ]

    datasource_config = DatasourceConfig(
        name="nida",
        version="v1.0"
    )
    assert validate_datasource_config(datasource_config, metadata, expected_data_format) == [
        "nida-12studies.tar.gz"
    ]


def test_validate_datasource_config_invalid(metadata):
    expected_data_format = "topmed"
    with pytest.raises(RuntimeError):
        datasource_config = DatasourceConfig(
            name="topmed",
            version="v1.0"
        )
        validate_datasource_config(datasource_config, metadata, expected_data_format)
    expected_data_format = "dbGaP"
    with pytest.raises(RuntimeError):
        datasource_config = DatasourceConfig(
            name="bdc",
            version="v2.0"
        )
        validate_datasource_config(datasource_config, metadata, expected_data_format)
