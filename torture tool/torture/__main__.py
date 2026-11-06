#!/usr/bin/env python3
"""
Package entrypoint enabling `python3 -m torture` and `python3 torture`.
"""

import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from torture.main import main

if __name__ == "__main__":
    sys.exit(main())


