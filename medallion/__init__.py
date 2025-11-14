"""
========================================================
Medallion Architecture Package
========================================================

Implements the three-layer medallion architecture for the data warehouse:
    - Bronze: Raw data ingestion layer
    - Silver: Cleansed and validated data layer
    - Gold: Business analytics layer

Modules:
    bronze: Bronze layer manager for raw data ingestion

Example:
    >>> from medallion.bronze import BronzeManager
    >>> 
    >>> manager = BronzeManager()
    >>> manager.load_all_crm_data()
"""

__version__ = "0.1.0"
__all__ = ['BronzeManager']

from medallion.bronze import BronzeManager
