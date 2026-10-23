"""
NYC 311 Data Factory Unit Test Suite.
100% offline, deterministic, and isolated from live network resources.
"""

from pathlib import Path
import sys

# Ensure src/ is automatically on sys.path for direct unittest discovery
src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))
