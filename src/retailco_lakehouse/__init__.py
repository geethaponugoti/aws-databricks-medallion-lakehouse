"""Reusable pipeline logic for the RetailCo medallion lakehouse.

Notebooks under notebooks/ are thin runners: they resolve configuration,
call into this package, and display the result. Everything with real logic
lives here so it can be unit tested with a local SparkSession.
"""
