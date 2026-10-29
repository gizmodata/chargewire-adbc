"""Renamed: this package is now adbc-driver-chargewire (module ``adbc_driver_chargewire``).

Importing it imports the renamed driver instead, with a DeprecationWarning. Its modules are
the renamed driver's own (``adbc_driver_warpsql.dbapi`` is ``adbc_driver_chargewire.dbapi``),
so everything they define, and the driver's library path, is the renamed driver's.
"""

import sys
import warnings

import adbc_driver_chargewire
from adbc_driver_chargewire import _driver, _options, dbapi
from adbc_driver_chargewire import ConnectionOptions, DatabaseOptions, StatementOptions

for _name, _module in (("_driver", _driver), ("_options", _options), ("dbapi", dbapi)):
    sys.modules[f"{__name__}.{_name}"] = _module

__version__ = "1.0.2"
__all__ = ["ConnectionOptions", "DatabaseOptions", "StatementOptions", "__version__"]

warnings.warn(
    "adbc-driver-warpsql has been renamed to adbc-driver-chargewire: install that and import "
    f"adbc_driver_chargewire (now {adbc_driver_chargewire.__version__})",
    DeprecationWarning,
    stacklevel=2,
)
