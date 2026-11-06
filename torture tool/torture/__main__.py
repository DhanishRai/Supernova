#!/usr/bin/env python3
"""
Package entrypoint enabling `python3 -m torture` and `python3 torture`.
"""

import sys
from torture.main import main

if __name__ == "__main__":
    sys.exit(main())

