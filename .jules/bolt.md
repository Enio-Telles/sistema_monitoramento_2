## 2025-03-19 - Replacing Polars `map_elements` with native string expressions
**Learning:** Polars' `map_elements` for string normalization (like removing accents and uppercasing) drops execution down to Python bytecode for every row, blocking vectorization and making operations extremely slow.
**Action:** Always prefer native Polars string expressions. For accent removal, use `.str.to_uppercase()` followed by `.str.replace_many()` with a mapped dictionary of uppercase accented characters to their unaccented equivalents. This can yield an order-of-magnitude performance improvement.
