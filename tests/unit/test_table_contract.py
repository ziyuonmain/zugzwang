"""Tests for the published table metadata contract."""

from pathlib import Path

import pytest
import yaml

from zugzwang.table_contract import (
    DEFAULT_CONTRACT_FILES,
    ColumnContract,
    load_table_contracts,
)


def _with_silver(path: Path) -> dict[str, Path]:
    return {'silver': path, 'gold': DEFAULT_CONTRACT_FILES['gold']}


def test_default_manifest_is_complete():
    """Loads every published table and column from the repository manifest."""
    contracts = load_table_contracts()

    assert len(contracts) == 7
    assert sum(len(contract.columns) for contract in contracts.values()) == 83
    assert contracts['train_stop_weather'].published_name == 'gold.train_stop_weather'
    assert all(contract.description for contract in contracts.values())
    assert all(
        column.description
        for contract in contracts.values()
        for column in contract.columns
    )


def test_column_contract_escapes_sql_comment():
    """Escapes apostrophes when rendering the Lakeflow DDL schema."""
    column = ColumnContract('example', 'string', "Publisher's value")

    assert column.to_ddl() == "example STRING COMMENT 'Publisher''s value'"


def test_manifest_defines_column_names(tmp_path: Path):
    """Treats YAML, rather than a duplicated Python list, as the source of truth."""
    manifest = yaml.safe_load(
        Path('metadata/silver_tables.yml').read_text(encoding='utf-8')
    )
    manifest['tables']['stations']['columns'][0]['name'] = 'unexpected_eva'
    path = tmp_path / 'silver_tables.yml'
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding='utf-8')

    contracts = load_table_contracts(_with_silver(path))

    assert contracts['stations'].columns[0].name == 'unexpected_eva'


def test_manifest_rejects_unsupported_type(tmp_path: Path):
    """Rejects types that cannot be rendered into the Lakeflow schema contract."""
    manifest = yaml.safe_load(
        Path('metadata/silver_tables.yml').read_text(encoding='utf-8')
    )
    manifest['tables']['stations']['columns'][0]['type'] = 'made_up_type'
    path = tmp_path / 'silver_tables.yml'
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding='utf-8')

    with pytest.raises(ValueError, match='unsupported value'):
        load_table_contracts(_with_silver(path))


def test_manifest_rejects_duplicate_yaml_key(tmp_path: Path):
    """Rejects YAML mappings whose duplicate keys would otherwise be overwritten."""
    path = tmp_path / 'silver_tables.yml'
    path.write_text('version: 1\nversion: 1\nschemas: {}\n', encoding='utf-8')

    with pytest.raises(ValueError, match='Duplicate YAML key: version'):
        load_table_contracts(_with_silver(path))
