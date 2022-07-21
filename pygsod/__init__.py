# Put everything in the global namespace so you can access them by attribute
# from pygsod import (gsod, utils)

from pkg_resources import get_distribution

# version number, from setup.py
__version__ = get_distribution("pygsod").version
