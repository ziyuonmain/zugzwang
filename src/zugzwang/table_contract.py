"""Load and validate published table metadata from YAML."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from yaml.resolver import BaseResolver

SUPPORTED_TYPES = {'bigint', 'boolean', 'double', 'int', 'string', 'timestamp'}
METADATA_DIR = Path(__file__).resolve().parents[2] / 'metadata'
DEFAULT_CONTRACT_FILES = {
    'silver': METADATA_DIR / 'silver_tables.yml',
    'gold': METADATA_DIR / 'gold_tables.yml',
}


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f'Duplicate YAML key: {key}')
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


@dataclass(frozen=True)
class ColumnContract:
    """Published column type and description."""

    name: str
    data_type: str
    description: str

    def to_ddl(self) -> str:
        """Return a SQL DDL column declaration with a comment."""
        description = self.description.replace("'", "''")
        return f"{self.name} {self.data_type.upper()} COMMENT '{description}'"


@dataclass(frozen=True)
class TableContract:
    """Published table identifier, description, and ordered columns."""

    logical_name: str
    published_name: str
    description: str
    columns: tuple[ColumnContract, ...]

    @property
    def schema_ddl(self) -> str:
        """Return the ordered SQL DDL schema consumed by Lakeflow."""
        return ',\n'.join(column.to_ddl() for column in self.columns)


def _mapping(value: object, location: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f'{location} must be a mapping with string keys')
    return value


def _text(value: object, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{location} must be a nonempty string')
    return value.strip()


def _only(mapping: Mapping[str, Any], allowed: set[str], location: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ValueError(f'{location} contains unknown keys: {", ".join(unknown)}')


def _load_file(schema: str, path: Path) -> dict[str, TableContract]:
    with path.open(encoding='utf-8') as stream:
        root = _mapping(yaml.load(stream, Loader=_UniqueKeyLoader), str(path))

    _only(root, {'version', 'tables'}, str(path))
    if root.get('version') != 1:
        raise ValueError(f'{path}.version must equal 1')

    contracts: dict[str, TableContract] = {}
    for table_name, table_value in _mapping(
        root.get('tables'), f'{path}.tables'
    ).items():
        table_location = f'{path}.tables.{table_name}'
        table_name = _text(table_name, table_location)
        table = _mapping(table_value, table_location)
        _only(table, {'description', 'columns'}, table_location)

        raw_columns = table.get('columns')
        if not isinstance(raw_columns, list) or not raw_columns:
            raise ValueError(f'{table_location}.columns must be a nonempty list')

        columns: list[ColumnContract] = []
        for index, column_value in enumerate(raw_columns):
            location = f'{table_location}.columns[{index}]'
            column = _mapping(column_value, location)
            _only(column, {'name', 'type', 'description'}, location)
            data_type = _text(column.get('type'), f'{location}.type').lower()
            if data_type not in SUPPORTED_TYPES:
                raise ValueError(f'{location}.type has unsupported value {data_type!r}')
            columns.append(
                ColumnContract(
                    name=_text(column.get('name'), f'{location}.name'),
                    data_type=data_type,
                    description=_text(
                        column.get('description'), f'{location}.description'
                    ),
                )
            )

        names = [column.name for column in columns]
        if len(names) != len(set(names)):
            raise ValueError(f'{table_location}.columns contains duplicate names')

        contracts[table_name] = TableContract(
            logical_name=table_name,
            published_name=table_name
            if schema == 'silver'
            else f'{schema}.{table_name}',
            description=_text(
                table.get('description'), f'{table_location}.description'
            ),
            columns=tuple(columns),
        )
    return contracts


def load_table_contracts(
    files: Mapping[str, Path] | None = None,
) -> dict[str, TableContract]:
    """Load and validate table metadata files keyed by target schema."""
    contracts: dict[str, TableContract] = {}
    for schema, path in (files or DEFAULT_CONTRACT_FILES).items():
        schema = _text(schema, 'schema name')
        schema_contracts = _load_file(schema, path)
        overlap = contracts.keys() & schema_contracts.keys()
        if overlap:
            raise ValueError(f'Tables declared in multiple schemas: {sorted(overlap)}')
        contracts.update(schema_contracts)
    return contracts


def main() -> None:
    """Validate the configured metadata files from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Validate table metadata.')
    if not parser.parse_args().check:
        parser.error('the --check flag is required')

    contracts = load_table_contracts()
    column_count = sum(len(contract.columns) for contract in contracts.values())
    print(f'Validated {len(contracts)} tables and {column_count} columns.')


if __name__ == '__main__':
    main()
