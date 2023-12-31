"""Tests standard target features using the built-in SDK tests library."""
# flake8: noqa

from __future__ import annotations

import copy
import io
import typing as t
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path

import jsonschema
import pytest
import sqlalchemy
from singer_sdk.testing import get_target_test_class, sync_end_to_end
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.types import TIMESTAMP, VARCHAR
from tests.samples.aapl.aapl import Fundamentals
from tests.samples.sample_tap_countries.countries_tap import SampleTapCountries

from target_mysql.connector import MySQLConnector
from target_mysql.target import TargetMySQL

SAMPLE_CONFIG: dict[str, t.Any] = {
    # Using 127.0.0.1 instead of localhost because of mysqlclient dialect.
    "sqlalchemy_url": "mysql+mysqldb://root:password@127.0.0.1:3306/melty",
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetMySQL,
    config=SAMPLE_CONFIG,
)


class TestTargetMySQL(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        return "resource"


@pytest.fixture(scope="session")
def mysql_config():
    return {
        "dialect+driver": "mysql+mysqldb",
        # Using 127.0.0.1 instead of localhost because of mysqlclient dialect.
        "host": "127.0.0.1",
        "user": "root",
        "password": "password",
        "database": "melty",
        "port": 3306,
        "add_record_metadata": True,
        "hard_delete": False,
        "default_target_schema": "melty",
        "max_varchar_size": 255,
    }


@pytest.fixture()
def mysql_target(mysql_config) -> TargetMySQL:
    return TargetMySQL(config=mysql_config)


def create_engine(target_mysql: TargetMySQL) -> sqlalchemy.engine.Engine:
    return TargetMySQL.default_sink_class.connector_class(
        config=target_mysql.config
    )._engine


def remove_metadata_columns(row: dict) -> dict:
    new_row = {}
    for column in row.keys():
        if not column.startswith("_sdc"):
            new_row[column] = row[column]
    return new_row


def singer_file_to_target(file_name, target) -> None:
    """Singer file to Target, emulates a tap run.

    Equivalent to running cat file_path | target-name --config config.json.
    Note that this function loads all lines into memory, so it is
    not good very large files.

    Args:
        file_name: name to file in .tests/data_files to be sent into target
        Target: Target to pass data from file_path into..
    """
    file_path = Path(__file__).parent / Path("./data_files") / Path(file_name)
    buf = io.StringIO()
    with redirect_stdout(buf), open(file_path) as f:
        for line in f:
            print(line.rstrip("\r\n"))  # File endings are here,
            # and print adds another line ending so we need to remove one.
    buf.seek(0)
    target.listen(buf)


# TODO should set schemas for each tap individually so we don't collide


def test_sqlalchemy_url_config(mysql_config):
    """Be sure that passing a sqlalchemy_url works.

    mysql_config is used because an SQLAlchemy URL will override all SSL
    settings and preclude connecting to a database using SSL.
    """
    host = mysql_config["host"]
    user = mysql_config["user"]
    password = mysql_config["password"]
    database = mysql_config["database"]
    port = mysql_config["port"]

    config = {
        "sqlalchemy_url": f"mysql+mysqldb://{user}:{password}@{host}:{port}/{database}",
    }
    tap = SampleTapCountries(config={}, state=None)
    target = TargetMySQL(config=config)
    sync_end_to_end(tap, target)


def test_port_default_config():
    """Test that the default config is passed into the engine when the config doesn't provide it."""
    config = {
        "dialect+driver": "mysql+mysqldb",
        # Using 127.0.0.1 instead of localhost because of mysqlclient dialect.
        "host": "127.0.0.1",
        "user": "root",
        "password": "password",
        "database": "melty",
    }
    dialect_driver = config["dialect+driver"]
    host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    target_config = TargetMySQL(config=config).config
    connector = MySQLConnector(target_config)

    engine: sqlalchemy.engine.Engine = connector.create_sqlalchemy_engine()
    assert (
        str(engine.url)
        == f"{dialect_driver}://{user}:{password}@{host}:3306/{database}"
    )


def test_port_config():
    """Test that the port config works."""
    config = {
        "dialect+driver": "mysql+mysqldb",
        # Using 127.0.0.1 instead of localhost because of mysqlclient dialect.
        "host": "127.0.0.1",
        "user": "root",
        "password": "pasword",
        "database": "melty",
        "port": 3306,
    }
    dialect_driver = config["dialect+driver"]
    host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    target_config = TargetMySQL(config=config).config
    connector = MySQLConnector(target_config)

    engine: sqlalchemy.engine.Engine = connector.create_sqlalchemy_engine()
    assert (
        str(engine.url)
        == f"{dialect_driver}://{user}:{password}@{host}:3306/{database}"
    )


# Test name would work well
def test_countries_to_mysql(mysql_config):
    tap = SampleTapCountries(config={}, state=None)
    target = TargetMySQL(config=mysql_config)
    sync_end_to_end(tap, target)


def test_aapl_to_mysql(mysql_config):
    tap = Fundamentals(config={}, state=None)
    target = TargetMySQL(config=mysql_config)
    sync_end_to_end(tap, target)


def test_record_before_schema(mysql_target):
    with pytest.raises(Exception) as e:
        file_name = "record_before_schema.singer"
        singer_file_to_target(file_name, mysql_target)

    assert (
        str(e.value) == "Schema message has not been sent for test_record_before_schema"
    )


def test_invalid_schema(mysql_target):
    with pytest.raises(Exception) as e:
        file_name = "invalid_schema.singer"
        singer_file_to_target(file_name, mysql_target)
    assert (
        str(e.value) == "Line is missing required properties key(s): {'type': 'object'}"
    )


def test_record_missing_key_property(mysql_target):
    with pytest.raises(Exception) as e:
        file_name = "record_missing_key_property.singer"
        singer_file_to_target(file_name, mysql_target)
    assert "Record is missing one or more key_properties." in str(e.value)


def test_record_missing_required_property(mysql_target):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        file_name = "record_missing_required_property.singer"
        singer_file_to_target(file_name, mysql_target)


def test_camelcase(mysql_target):
    file_name = "camelcase.singer"
    singer_file_to_target(file_name, mysql_target)


def test_special_chars_in_attributes(mysql_target):
    file_name = "special_chars_in_attributes.singer"
    singer_file_to_target(file_name, mysql_target)


# TODO test that data is correctly set
def test_optional_attributes(mysql_target):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, mysql_target)


def test_schema_no_properties(mysql_target):
    """Expect to fail with ValueError."""
    file_name = "schema_no_properties.singer"
    singer_file_to_target(file_name, mysql_target)


# TODO test that data is correct
def test_schema_updates(mysql_target):
    file_name = "schema_updates.singer"
    singer_file_to_target(file_name, mysql_target)


# TODO test that data is correct
def test_multiple_state_messages(mysql_target):
    file_name = "multiple_state_messages.singer"
    singer_file_to_target(file_name, mysql_target)


def test_relational_data(mysql_target):
    engine = create_engine(mysql_target)
    file_name = "user_location_data.singer"
    singer_file_to_target(file_name, mysql_target)

    file_name = "user_location_upsert_data.singer"
    singer_file_to_target(file_name, mysql_target)

    schema_name = mysql_target.config["default_target_schema"]

    with engine.connect() as connection:
        expected_test_users = [
            {"id": 1, "name": "Johny"},
            {"id": 2, "name": "George"},
            {"id": 3, "name": "Jacob"},
            {"id": 4, "name": "Josh"},
            {"id": 5, "name": "Jim"},
            {"id": 8, "name": "Thomas"},
            {"id": 12, "name": "Paul"},
            {"id": 13, "name": "Mary"},
        ]

        full_table_name = f"{schema_name}.test_users"
        result = connection.execute(f"SELECT * FROM {full_table_name} ORDER BY id")
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_test_users

        expected_test_locations = [
            {"id": 1, "name": "Philly"},
            {"id": 2, "name": "NY"},
            {"id": 3, "name": "San Francisco"},
            {"id": 6, "name": "Colorado"},
            {"id": 8, "name": "Boston"},
        ]

        full_table_name = f"{schema_name}.test_locations"
        result = connection.execute(f"SELECT * FROM {full_table_name} ORDER BY id")
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_test_locations

        expected_test_user_in_location = [
            {
                "id": 1,
                "user_id": 1,
                "location_id": 4,
                # These are returned as strings by the connection.execute() call below,
                # despite being stored as JSON columns in the backend.
                "info": '{"mood": "sad", "weather": "rainy"}',
            },
            {
                "id": 2,
                "user_id": 2,
                "location_id": 3,
                "info": '{"mood": "satisfied", "weather": "sunny"}',
            },
            {
                "id": 3,
                "user_id": 1,
                "location_id": 3,
                "info": '{"mood": "happy", "weather": "sunny"}',
            },
            {
                "id": 6,
                "user_id": 3,
                "location_id": 2,
                "info": '{"mood": "happy", "weather": "sunny"}',
            },
            {
                "id": 14,
                "user_id": 4,
                "location_id": 1,
                "info": '{"mood": "ok", "weather": "cloudy"}',
            },
        ]

        full_table_name = f"{schema_name}.test_user_in_location"
        result = connection.execute(f"SELECT * FROM {full_table_name} ORDER BY id")
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_test_user_in_location


def test_no_primary_keys(mysql_target):
    """We run both of these tests twice just to ensure that no records are removed and append only works properly."""
    engine = create_engine(mysql_target)
    table_name = "test_no_pk"
    full_table_name = mysql_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, mysql_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, mysql_target)

    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, mysql_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, mysql_target)

    # Will populate us with 22 records, we run this twice
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 16


# TODO test that data is correct
def test_duplicate_records(mysql_target):
    file_name = "duplicate_records.singer"
    singer_file_to_target(file_name, mysql_target)


# TODO test that data is correct
def test_array_data(mysql_target):
    file_name = "array_data.singer"
    singer_file_to_target(file_name, mysql_target)


# TODO test that data is correct
def test_encoded_string_data(mysql_target):
    """We removed NUL characters from the original encoded_strings.singer as postgres doesn't allow them.
    https://www.postgresql.org/docs/current/functions-string.html#:~:text=chr(0)%20is%20disallowed%20because%20text%20data%20types%20cannot%20store%20that%20character.
    chr(0) is disallowed because text data types cannot store that character.

    Note you will recieve a  ValueError: A string literal cannot contain NUL (0x00) characters. Which seems like a reasonable error.
    See issue https://github.com/MeltanoLabs/target-postgres/issues/60 for more details.
    """
    file_name = "encoded_strings.singer"
    singer_file_to_target(file_name, mysql_target)


def test_tap_appl(mysql_target):
    """Expect to fail with ValueError due to primary key https://github.com/MeltanoLabs/target-postgres/issues/54."""
    file_name = "tap_aapl.singer"
    singer_file_to_target(file_name, mysql_target)


def test_tap_countries(mysql_target):
    file_name = "tap_countries.singer"
    singer_file_to_target(file_name, mysql_target)


def test_missing_value(mysql_target):
    file_name = "missing_value.singer"
    singer_file_to_target(file_name, mysql_target)


def test_large_int(mysql_target):
    file_name = "large_int.singer"
    singer_file_to_target(file_name, mysql_target)


def test_decimal_data(mysql_target):
    "Test that decimals (number type in jsonschema) is handled correctly"
    file_name = "decimal_data.singer"
    singer_file_to_target(file_name, mysql_target)

    engine = create_engine(mysql_target)
    schema_name = mysql_target.config["default_target_schema"]

    with engine.connect() as connection:
        expected_decimal_data = [
            {"id": 1, "amount": Decimal("4.0")},
            {"id": 2, "amount": Decimal("5.6")},
            {"id": 3, "amount": Decimal("-0.0000000007")},
            {"id": 4, "amount": Decimal("812345678.9")},
        ]

        full_table_name = f"{schema_name}.test_decimal_data"
        result = connection.execute(f"SELECT * FROM {full_table_name} ORDER BY id")
        result_dict = [remove_metadata_columns(row._asdict()) for row in result.all()]
        assert result_dict == expected_decimal_data


def test_anyof(mysql_target):
    """Test that anyOf is handled correctly"""
    file_name = f"commits.singer"
    singer_file_to_target(file_name, mysql_target)

    engine = create_engine(mysql_target)
    schema_name = mysql_target.config["default_target_schema"]
    with engine.connect() as connection:
        meta = sqlalchemy.MetaData(bind=connection)
        table = sqlalchemy.Table("commits", meta, schema=schema_name, autoload=True)
        for column in table.c:
            # {"type":"string"}
            if column.name == "id":
                assert isinstance(column.type, VARCHAR)

            # Any of nullable date-time.
            # {"anyOf":[{"type":"string","format":"date-time"},{"type":"null"}]}
            if column.name in {"authored_date", "committed_date"}:
                assert isinstance(column.type, TIMESTAMP)

            # Any of nullable array of strings or single string.
            # {"anyOf":[{"type":"array","items":{"type":["null","string"]}},{"type":"string"},{"type":"null"}]}
            if column.name == "parent_ids":
                assert isinstance(column.type, JSON)

            # Any of nullable string.
            # {"anyOf":[{"type":"string"},{"type":"null"}]}
            if column.name == "commit_message":
                assert isinstance(column.type, VARCHAR)

            # Any of nullable string or integer.
            # {"anyOf":[{"type":"string"},{"type":"integer"},{"type":"null"}]}
            if column.name == "legacy_id":
                assert isinstance(column.type, VARCHAR)


# TODO: Reimplement test_reserved_keywords. See target-postgres for an example.
# Originally removed because the large number of columns were overflowing the MySQL
# maximum row size of 65535 bytes.


def test_new_array_column(mysql_target):
    """Create a new Array column with an existing table."""
    file_name = "new_array_column.singer"
    singer_file_to_target(file_name, mysql_target)


def test_activate_version_hard_delete(mysql_config):
    """Activate Version Hard Delete Test."""
    table_name = "test_activate_version_hard"
    file_name = f"{table_name}.singer"
    full_table_name = mysql_config["default_target_schema"] + "." + table_name
    mysql_config_hard_delete_true = copy.deepcopy(mysql_config)
    mysql_config_hard_delete_true["hard_delete"] = True
    mysql_hard_delete_true = TargetMySQL(config=mysql_config_hard_delete_true)
    engine = create_engine(mysql_hard_delete_true)
    singer_file_to_target(file_name, mysql_hard_delete_true)
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 7
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, `name`) VALUES('Manual1', 'Meltano')",
        )
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, `name`) VALUES('Manual2', 'Meltano')",
        )
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

    singer_file_to_target(file_name, mysql_hard_delete_true)

    # Should remove the 2 records we added manually
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 7


def test_activate_version_soft_delete(mysql_target):
    """Activate Version Soft Delete Test."""
    engine = create_engine(mysql_target)
    table_name = "test_activate_version_soft"
    file_name = f"{table_name}.singer"
    full_table_name = mysql_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    mysql_config_soft_delete = copy.deepcopy(mysql_target._config)
    mysql_config_soft_delete["hard_delete"] = False
    pg_soft_delete = TargetMySQL(config=mysql_config_soft_delete)
    singer_file_to_target(file_name, pg_soft_delete)

    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 7
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, `name`) VALUES('Manual1', 'Meltano')",
        )
        result = connection.execute(
            f"INSERT INTO {full_table_name}(code, `name`) VALUES('Manual2', 'Meltano')",
        )
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

    singer_file_to_target(file_name, pg_soft_delete)

    # Should have all records including the 2 we added manually
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

        result = connection.execute(
            f"SELECT * FROM {full_table_name} where _sdc_deleted_at is NOT NULL",
        )
        assert result.rowcount == 2


def test_activate_version_deletes_data_properly(mysql_target):
    """Activate Version should."""
    engine = create_engine(mysql_target)
    table_name = "test_activate_version_deletes_data_properly"
    file_name = f"{table_name}.singer"
    full_table_name = mysql_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection:
        result = connection.execute(f"DROP TABLE IF EXISTS {full_table_name}")

    mysql_config_soft_delete = copy.deepcopy(mysql_target._config)
    mysql_config_soft_delete["hard_delete"] = True
    pg_hard_delete = TargetMySQL(config=mysql_config_soft_delete)
    singer_file_to_target(file_name, pg_hard_delete)
    # Will populate us with 7 records
    with engine.connect() as connection:
        result = connection.execute(
            f"INSERT INTO {full_table_name} (code, `name`) VALUES('Manual1', 'Meltano')",
        )
        result = connection.execute(
            f"INSERT INTO {full_table_name} (code, `name`) VALUES('Manual2', 'Meltano')",
        )
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 9

    # Only has a schema and one activate_version message, should delete all records as it's a higher version than what's currently in the table
    file_name = f"{table_name}_2.singer"
    singer_file_to_target(file_name, pg_hard_delete)
    with engine.connect() as connection:
        result = connection.execute(f"SELECT * FROM {full_table_name}")
        assert result.rowcount == 0
