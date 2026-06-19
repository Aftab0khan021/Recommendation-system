"""
conftest.py — adds the backend directory to sys.path so pytest can import
modules like `ab_testing`, `cache`, `search_engine` etc. directly.
"""
import sys
import os

# Add backend root to path (parent of the tests/ directory)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
