"""Pure DataFrame-in, DataFrame-out transformations for each medallion layer.

Every function here takes a Bronze/Silver DataFrame and returns a new
DataFrame — no reads from or writes to tables, no widgets, no dbutils. That
makes them straightforward to unit test with a local SparkSession and small
in-memory DataFrames; the notebooks that call them own all the I/O.
"""
