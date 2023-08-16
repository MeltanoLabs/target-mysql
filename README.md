# target-mysql

`target-mysql` is a Singer target for MySQL.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

Install from GitHub:

```bash
pipx install git+https://github.com/MeltanoLabs/target-mysql.git@main
```

## Configuration

### Accepted Config Options

| Setting              | Required | Default | Description |
|:---------------------|:--------:|:-------:|:------------|
| host                 | False    | None    | Hostname for MySQL instance. Note if sqlalchemy_url is set this will be ignored. |
| port                 | False    |    3306 | The port on which MySQL is awaiting connection. Note if sqlalchemy_url is set this will be ignored. |
| user                 | False    | None    | User name used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| password             | False    | None    | Password used to authenticate. Note if sqlalchemy_url is set this will be ignored. |
| database             | False    | None    | Database name. Note if sqlalchemy_url is set this will be ignored. |
| sqlalchemy_url       | False    | None    | SQLAlchemy connection string. This will override using host, user, password, port, dialect, and all ssl settings. Note that you must escape password special characters properly. See https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords |
| dialect+driver       | False    | mysql+mysqldb | Dialect+driver see https://docs.sqlalchemy.org/en/20/core/engines.html. Generally just leave this alone. Note if sqlalchemy_url is set this will be ignored. |
| default_target_schema| False    | melty   | MySQL schema to send data to, example: tap-clickup |
| hard_delete          | False    |       0 | When activate version is sent from a tap this specefies if we should delete the records that don't match, or mark them with a date in the `_sdc_deleted_at` column. |
| add_record_metadata  | False    |       1 | Note that this must be enabled for activate_version to work!This adds _sdc_extracted_at, _sdc_batched_at, and more to every table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html for more information. |
| max_varchar_size     | False    |     255 | Determines the maximum size of non-primary-key VARCHAR() fields. Keep in mind that each row in a MySQL table has a maximum size of 65535 bytes. |
| stream_maps          | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled   | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-mysql --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `target-mysql` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-mysql --version
target-mysql --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-mysql --config /path/to/target-mysql-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-mysql` CLI interface directly using `poetry run`:

```bash
poetry run target-mysql --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-mysql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-mysql --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-mysql
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
