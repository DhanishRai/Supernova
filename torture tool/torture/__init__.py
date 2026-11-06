"""
Torture Tool - Crash-Consistency Testing Suite for Supernova Engine.
Standard library only; zero external dependencies.
"""

import signal

# Windows compatibility: expose SIGKILL attribute (maps to signal 9 / process termination)
if not hasattr(signal, "SIGKILL"):
    signal.SIGKILL = 9

__version__ = "0.1.0"
