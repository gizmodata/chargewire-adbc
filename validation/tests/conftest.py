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

"""The validation suite's fixtures, for this driver.

This suite gates releases, so it fails, never skips, when its server or driver is missing:
the upstream fixtures skip a test whose environment variable is unset, so the variables are
checked here first, and the server is dialed (and given the secondary schema and catalog the
suite needs) before any test runs. A run in which no test ran fails too (pytest exit 5).
"""

import os
from pathlib import Path

import adbc_driver_manager.dbapi
import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from . import chargewire


def pytest_addoption(parser: pytest.Parser) -> None:
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)


@pytest.fixture(scope="session")
def driver(request: pytest.FixtureRequest) -> adbc_drivers_validation.model.DriverQuirks:
    assert request.param.startswith("chargewire")
    return chargewire.get_quirks()


@pytest.fixture(scope="session")
def driver_path() -> str:
    return chargewire.driver_path()


@pytest.fixture(scope="session", autouse=True)
def chargewire_server() -> None:
    """Fail (not skip) without the server; create what the suite expects on it."""
    missing = [
        env
        for env in (chargewire.URI_ENV, chargewire.USERNAME_ENV, chargewire.PASSWORD_ENV)
        if not os.environ.get(env)
    ]
    if missing:
        pytest.fail(reason=f"set {', '.join(missing)}: the validation suite needs a server")
    path = chargewire.driver_path()
    if not Path(path).is_file():
        pytest.fail(reason=f"no driver library at {path}: run `make -C go lib` first")
    options = {
        "uri": os.environ[chargewire.URI_ENV],
        "username": os.environ[chargewire.USERNAME_ENV],
        "password": os.environ[chargewire.PASSWORD_ENV],
    }
    try:
        with (
            adbc_driver_manager.dbapi.connect(
                driver=path, db_kwargs=options, autocommit=True
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(operation="SELECT current_database()")
            catalog = cursor.fetchone()[0]
            if catalog != chargewire.CATALOG:
                pytest.fail(
                    reason=f"the server's catalog is {catalog!r}, not {chargewire.CATALOG!r}: "
                    f"start it with --database <dir>/{chargewire.CATALOG}.duckdb"
                )
            cursor.execute(operation="SELECT current_setting('TimeZone')")
            timezone = cursor.fetchone()[0]
            if timezone != "UTC":
                pytest.fail(
                    reason=f"the server's TimeZone is {timezone!r}, not 'UTC': the suite expects "
                    "TIMESTAMPTZ results as timestamp[tz=UTC]; start the server with TZ=UTC"
                )
            cursor.execute(
                operation=f"CREATE SCHEMA IF NOT EXISTS "
                f"{chargewire.CATALOG}.{chargewire.SECONDARY_SCHEMA}"
            )
            cursor.execute(
                operation=f"ATTACH IF NOT EXISTS ':memory:' AS {chargewire.SECONDARY_CATALOG}"
            )
    except adbc_driver_manager.dbapi.Error as error:
        pytest.fail(reason=f"can't reach the server at {options['uri']}: {error}")
