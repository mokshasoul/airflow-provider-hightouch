"""
Version module for the package.
"""

import os
import sys

__version__ = "4.0.0"


def validate_version():
    """
    Validates the version of the package against the Git tag.

    Raises:
        SystemExit: If the Git tag does not match the version.
    """
    version = __version__
    tag = os.getenv("CIRCLE_TAG")
    if tag != version:
        info = f"Git tag: {tag} does not match the version : {version}"
        sys.exit(info)


if __name__ == "__main__":
    validate_version()
