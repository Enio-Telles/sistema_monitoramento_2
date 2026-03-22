
## 2024-05-18 - [Pandas DataFrame __getitem__ Overhead]
**Learning:** In Pandas DataFrames, accessing columns in a loop like `df_pd[col_name]` adds up due to `__getitem__` overhead. If accessing multiple times, it is better to cache `col_data = df_pd[col_name]`.
**Action:** When iterating over a dataframe's columns and repeatedly reading column-level properties, cache the column data to optimize.
