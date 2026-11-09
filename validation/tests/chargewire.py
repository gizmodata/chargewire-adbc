# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 GizmoData LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""What the ADBC Driver Foundry validation suite should expect of this driver.

Adapted from adbc-drivers/quack's and adbc-drivers/mysql's validation quirks. Every feature
the driver implements is switched on; a feature switched off, and every query override under
../queries, says why in a comment. The server under test is Hardcharger, the Chargewire server
the other live suites run against (GizmoData's DuckDB 2.0 server); the skips and expected failures
that name it are its behavior, not the driver's.
"""

import functools
import os
import re
from pathlib import Path

from adbc_drivers_validation import model, quirks

# The server under test: a hardcharger-server started with `--database <dir>/validation.duckdb`
# (so its catalog is `validation`), reachable at CHARGEWIRE_ADBC_TEST_VALIDATION_URI. The suite's
# environment variables carry this project's prefix, never a generic HARDCHARGER_* or
# CHARGEWIRE_* name a developer's shell may export for a real server.
URI_ENV = "CHARGEWIRE_ADBC_TEST_VALIDATION_URI"
USERNAME_ENV = "CHARGEWIRE_ADBC_TEST_VALIDATION_USERNAME"
PASSWORD_ENV = "CHARGEWIRE_ADBC_TEST_VALIDATION_PASSWORD"
DRIVER_PATH_ENV = "CHARGEWIRE_ADBC_TEST_VALIDATION_DRIVER_PATH"

CATALOG = "validation"
SECONDARY_SCHEMA = "validation_secondary"
SECONDARY_CATALOG = "validation_secondary_catalog"


class ChargewireQuirks(model.DriverQuirks):
    name = "chargewire"
    driver = "adbc_driver_chargewire"
    driver_name = "ADBC Chargewire Driver - Go"
    # GetInfo's vendor name and version are the server's (its discovery document).
    vendor_name = "Hardcharger"
    # GetInfo's vendor version is the server's and its DuckDB's: "2.10.0 (DuckDB v2.0.0)".
    vendor_version = re.compile(r"2\.\d+\.\d+\S* \(DuckDB v2\.\d+\.\d+\S*\)\Z")
    short_version = "2"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        # GetStatistics is not implemented (NOT_IMPLEMENTED): Chargewire's metadata endpoints
        # (catalogs, db-schemas, tables, table-types, primary-keys, imported-keys) carry no
        # statistics.
        connection_get_statistics=False,
        connection_set_current_catalog=True,
        connection_set_current_schema=True,
        connection_transactions=True,
        get_objects=True,
        # GetObjects reads constraints from Chargewire's metadata endpoints, which report
        # primary keys and foreign keys (imported-keys) only: the protocol has no endpoint
        # for CHECK or UNIQUE constraints, so they are not reported.
        get_objects_constraints_check=False,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        get_objects_constraints_unique=False,
        get_objects_tabletypes_filter=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=True,
        statement_bulk_ingest_schema=True,
        statement_bulk_ingest_temporary=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        # DDL reports -1 rows affected (unknown), which ADBC allows; DuckDB counts no rows for it.
        statement_rows_affected_ddl=False,
        current_catalog=CATALOG,
        current_schema="main",
        secondary_catalog=SECONDARY_CATALOG,
        secondary_catalog_schema="main",
        secondary_schema=SECONDARY_SCHEMA,
        # The xdbc_* fields GetObjects fills for a column (from the tables endpoint's column
        # metadata).
        supported_xdbc_fields=[
            "xdbc_data_type",
            "xdbc_sql_data_type",
            "xdbc_type_name",
            "xdbc_column_size",
            "xdbc_decimal_digits",
            "xdbc_num_prec_radix",
            "xdbc_nullable",
            "xdbc_is_nullable",
            "xdbc_is_autoincrement",
        ],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv(URI_ENV),
            "username": model.FromEnv(USERNAME_ENV),
            "password": model.FromEnv(PASSWORD_ENV),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        text = str(error).lower()
        if "does not exist" not in text and "not found" not in text:
            return False
        return table_name is None or table_name.lower() in text

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="duckdb")

    @property
    def sample_ddl_constraints(self) -> list[str]:
        # DuckDB can't drop a column a constraint names, so unlike other vendors' samples
        # the constrained columns aren't moved off ordinal position 1.
        return [
            (
                "CREATE TABLE constraint_check (a INTEGER, b INTEGER, "
                "CHECK (a > 0), CHECK (a + b > 0))"
            ),
            "CREATE TABLE constraint_primary (a INTEGER PRIMARY KEY, b VARCHAR)",
            "CREATE TABLE constraint_primary_multi (a INTEGER, b VARCHAR, PRIMARY KEY (b, a))",
            "CREATE TABLE constraint_primary_multi2 (a VARCHAR, b INTEGER, PRIMARY KEY (a, b))",
            (
                "CREATE TABLE constraint_unique (a INTEGER, b INTEGER, c INTEGER, "
                "UNIQUE (a), UNIQUE (c, b))"
            ),
            (
                "CREATE TABLE constraint_foreign (a INTEGER, b INTEGER, "
                "FOREIGN KEY (b) REFERENCES constraint_primary (a))"
            ),
            (
                "CREATE TABLE constraint_foreign_multi (a INTEGER, b INTEGER, c VARCHAR, "
                "FOREIGN KEY (c, b) REFERENCES constraint_primary_multi2 (a, b))"
            ),
        ]

    def qualify_temp_table(self, cursor, name: str) -> list[str]:
        # DuckDB keeps temporary tables in the `temp` catalog's `main` schema.
        return ["temp", "main", name]


def driver_path() -> str:
    """The driver library under test: CHARGEWIRE_ADBC_TEST_VALIDATION_DRIVER_PATH, or the one
    `make -C go lib` / scripts/build_lib.sh builds."""
    path = os.environ.get(DRIVER_PATH_ENV)
    if path:
        return path
    return str(
        Path(__file__).resolve().parent.parent.parent
        / "go"
        / "build"
        / f"libadbc_driver_chargewire.{model.DRIVER_EXTENSION}"
    )


@functools.cache
def get_quirks() -> ChargewireQuirks:
    return ChargewireQuirks()
