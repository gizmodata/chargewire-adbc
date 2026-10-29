"""Wheel configuration for adbc-driver-chargewire.

The package is pure Python but ships the prebuilt Go shared library
(``libadbc_driver_chargewire``), so wheels are platform-specific while staying
``py3-none`` (no CPython ABI dependency), e.g.
``adbc_driver_chargewire-X.Y.Z-py3-none-macosx_11_0_arm64.whl``.

``has_ext_modules`` makes setuptools use the platlib layout, so package
files land at the wheel root instead of a ``.data/purelib/`` tree, which a
wheel carrying a shared library needs (and auditwheel expects).
"""

from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class BinaryBdistWheel(bdist_wheel):
    def get_tag(self):
        _, _, plat = super().get_tag()
        return "py3", "none", plat


setup(distclass=BinaryDistribution, cmdclass={"bdist_wheel": BinaryBdistWheel})
