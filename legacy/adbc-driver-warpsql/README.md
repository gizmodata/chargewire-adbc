# adbc-driver-warpsql has been renamed

This package is now [`adbc-driver-chargewire`](https://pypi.org/project/adbc-driver-chargewire/).

```bash
pip install adbc-driver-chargewire
```

```python
from adbc_driver_chargewire import dbapi
```

This last release installs `adbc-driver-chargewire` and forwards `import adbc_driver_warpsql` to it
with a `DeprecationWarning`, so existing code keeps running. Connection strings and option keys
written for this package still work.
