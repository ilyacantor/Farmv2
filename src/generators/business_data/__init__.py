"""
Business data generators for enterprise source systems.

Generates realistic CRM, ERP, Billing, HCM, Support, PM, Monitoring, and Cloud Cost
data shaped the way each source system actually stores it. This data feeds the DCL
ingestion pipeline with live, semantically unified data.

Generator classes are imported from their individual modules (e.g.
``from src.generators.business_data.salesforce import SalesforceGenerator``).
This package __init__ intentionally does NOT re-export them so that importing
any single sub-module does not force-load all generators into memory.
"""
