## 2024-03-24 - [Optimize Excel export format initialization]
**Learning:** Initializing format columns based on individual series dtype instead of extracting dtype from DataFrame column is faster. The `col_data = df_pd[col_name]` allows reusing series data and avoiding multiple dictionary lookups on the dataframe.
**Action:** Always prefer caching accessed series data inside column loops for formatting and optimization.
