"""
Shared pytest configuration and fixtures for all tests.
"""

import sys
from pathlib import Path

import pytest

# Add project root to sys.path to enable importing project modules
# This allows tests to import from 'setup', 'core', 'sql', etc. without installation
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def pytest_configure(config):
    """Register custom markers for test categorization."""
    config.addinivalue_line("markers", "unit: Unit tests - isolated function-level tests")
    config.addinivalue_line("markers", "integration: Integration tests - component interactions")
    config.addinivalue_line("markers", "smoke: Smoke tests - basic functionality checks")
    config.addinivalue_line("markers", "edge_case: Edge case tests - boundary conditions")
    config.addinivalue_line("markers", "regression: Regression tests - previously fixed bugs")
    config.addinivalue_line("markers", "system: System tests - full system behavior tests")
    config.addinivalue_line("markers", "e2e: End-to-end tests - complete workflow tests")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle ipytest temporary files."""
    # ipytest creates temporary files named t_<hash>.py which should be collected
    pass


# NOTE: We DO want to collect ipytest temporary test files (t_*.py)
# So we do NOT ignore them
# collect_ignore_glob = ["t_*.py"]  # DISABLED to allow ipytest collection
