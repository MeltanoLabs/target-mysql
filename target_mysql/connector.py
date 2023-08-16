"""Connector class for target."""
from __future__ import annotations

from typing import cast

import sqlalchemy
from singer_sdk import SQLConnector
from singer_sdk import typing as th
from sqlalchemy.dialects.mysql import BIGINT, JSON
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.types import (
    BOOLEAN,
    DATE,
    DATETIME,
    DECIMAL,
    INTEGER,
    TIME,
    TIMESTAMP,
    VARCHAR,
)


class MySQLConnector(SQLConnector):
    """Sets up SQL Alchemy, and other MySQL related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def __init__(self, config: dict | None = None) -> None:
        """Initialize a connector to a MySQL database.

        Args:
            config: Configuration for the connector. Defaults to None.
        """
        url: URL = make_url(self.get_sqlalchemy_url(config=config))

        super().__init__(
            config,
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def prepare_table(  # noqa: PLR0913
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str],
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sqlalchemy.Table:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(bind=self._engine, schema=schema_name)
        if not self.table_exists(full_table_name=full_table_name):
            return self.create_empty_table(
                table_name=table_name,
                meta=meta,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
            )
        for property_name, property_def in schema["properties"].items():
            is_primary_key = property_name in primary_keys
            self.prepare_column(
                full_table_name,
                property_name,
                self.to_sql_type(
                    property_def,
                    self.config["max_varchar_size"],
                    is_primary_key,
                ),
            )
        meta.reflect(only=[table_name])

        return meta.tables[full_table_name]

    def create_sqlalchemy_connection(self) -> sqlalchemy.engine.Connection:
        """Return a new SQLAlchemy connection using the provided config.

        Read more details about why this doesn't work on postgres here.
        DML/DDL doesn't work with this being on according to these docs

        https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return self.create_sqlalchemy_engine().connect()

    def drop_table(self, table: sqlalchemy.Table) -> None:
        """Drop table data."""
        table.drop(bind=self.connection)

    # TODO: type hinting for arguments.
    def clone_table(  # noqa: PLR0913
        self,
        new_table_name,  # noqa: ANN001
        table,  # noqa: ANN001
        metadata,  # noqa: ANN001
        connection,  # noqa: ANN001
        temp_table,  # noqa: ANN001
    ) -> sqlalchemy.Table:
        """Clone a table."""
        new_columns = [
            sqlalchemy.Column(column.name, column.type) for column in table.columns
        ]

        if temp_table is True:
            new_table = sqlalchemy.Table(
                new_table_name,
                metadata,
                *new_columns,
                prefixes=["TEMPORARY"],
            )
        else:
            new_table = sqlalchemy.Table(new_table_name, metadata, *new_columns)
        new_table.create(bind=connection)
        return new_table

    @staticmethod
    def to_sql_type(
        jsonschema_type: dict,
        max_varchar_size: int = 255,
        is_primary_key: bool = False,
    ) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.
        If overriding this method, developers should call the default implementation
        from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.
            max_varchar_size: An upper limit on the size of varchar fields.
            is_primary_key: Whether the field represented by jsonschema_type is a
                primary key.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        json_type_array = []

        if jsonschema_type.get("type", False):
            if type(jsonschema_type["type"]) is str:
                json_type_array.append(jsonschema_type)
            elif type(jsonschema_type["type"]) is list:
                for entry in jsonschema_type["type"]:
                    json_type_dict = {}
                    json_type_dict["type"] = entry
                    if jsonschema_type.get("format", False):
                        json_type_dict["format"] = jsonschema_type["format"]
                    json_type_array.append(json_type_dict)
            else:
                msg = "Invalid format for jsonschema type: not str or list."
                raise RuntimeError(msg)
        elif jsonschema_type.get("anyOf", False):
            json_type_array = list(jsonschema_type["anyOf"])
        else:
            msg = "Neither type nor anyOf are present. Unable to determine type."
            raise RuntimeError(msg)

        sql_type_array = []
        for json_type in json_type_array:
            picked_type = MySQLConnector.pick_individual_type(jsonschema_type=json_type)
            if picked_type is not None:
                sql_type_array.append(picked_type)

        return MySQLConnector.pick_best_sql_type(
            sql_type_array=sql_type_array,
            is_primary_key=is_primary_key,
            max_varchar_size=max_varchar_size,
        )

    @staticmethod
    def pick_individual_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Select the correct sql type assuming jsonschema_type has only a single type.

        Args:
            jsonschema_type: A jsonschema_type array containing only a single type.

        Returns:
            An instance of the appropriate SQL type class based on jsonschema_type.
        """
        if "null" in jsonschema_type["type"]:
            return None
        if "integer" in jsonschema_type["type"]:
            return BIGINT()
        if "decimal" in jsonschema_type["type"]:
            return BIGINT()
        if "object" in jsonschema_type["type"]:
            return JSON()
        if "array" in jsonschema_type["type"]:
            return JSON()
        if jsonschema_type.get("format") == "date-time":
            return TIMESTAMP()
        return th.to_sql_type(jsonschema_type)

    @staticmethod
    def pick_best_sql_type(
        sql_type_array: list,
        is_primary_key: bool,
        max_varchar_size: int,
    ) -> sqlalchemy.types.TypeEngine:
        """Select the best SQL type from an array of instances of SQL type classes.

        Args:
            sql_type_array: The array of instances of SQL type classes.
            is_primary_key: Whether the field in question is a primary key.
            max_varchar_size: An upper limit on the size of varchar fields.

        Returns:
            An instance of the best SQL type class based on defined precedence order.
        """
        precedence_order = [
            JSON,
            VARCHAR,
            TIMESTAMP,
            DATETIME,
            DATE,
            TIME,
            DECIMAL,
            BIGINT,
            INTEGER,
            BOOLEAN,
        ]

        # Documentation seems conflicted on maximum size of a MySQL primary key field
        # and I couldn't immediately find an authoritative source, but there *is* a
        # maximum. The most convervative maximum size I found was 767 bytes.
        # Based on each utf8mb4 character being 4 bytes, 767/3~=191.
        # Could also potentially be a configuration option?
        max_varchar_primary_key_size = 191

        for sql_type in precedence_order:
            for obj in sql_type_array:
                if isinstance(obj, sql_type):
                    if isinstance(obj, VARCHAR):
                        if is_primary_key:
                            return VARCHAR(length=max_varchar_primary_key_size)
                        return VARCHAR(length=max_varchar_size)
                    return obj
        if is_primary_key:
            return VARCHAR(length=max_varchar_primary_key_size)
        return VARCHAR(length=max_varchar_size)

    def create_empty_table(  # noqa: PLR0913
        self,
        table_name: str,
        meta: sqlalchemy.MetaData,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,  # noqa: ARG002
        as_temp_table: bool = False,
    ) -> sqlalchemy.Table:
        """Create an empty target table.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
            meta: Metadata for the table.
            table_name: The name of the empty table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = (
                f"Schema for table_name: '{table_name}'does not define properties: "
                f"{schema}"
            )
            raise RuntimeError(msg) from e

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sqlalchemy.Column(
                    property_name,
                    self.to_sql_type(
                        property_jsonschema,
                        self.config["max_varchar_size"],
                        is_primary_key,
                    ),
                    primary_key=is_primary_key,
                ),
            )
        if as_temp_table:
            new_table = sqlalchemy.Table(
                table_name,
                meta,
                *columns,
                prefixes=["TEMPORARY"],
            )
            new_table.create(bind=self.connection)
            return new_table

        new_table = sqlalchemy.Table(table_name, meta, *columns)
        new_table.create(bind=self.connection)
        return new_table

    def get_column_add_ddl(
        self,
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sqlalchemy.Column(column_name, column_type)

        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ADD COLUMN %(column_name)s %(column_type)s",
            {
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast(str, config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername=config["dialect+driver"],
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
        )
        return cast(str, sqlalchemy_url)
