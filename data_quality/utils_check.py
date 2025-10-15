from pyspark.sql.functions import count, col, to_date
def check_dataframe_not_empty(df, name):
    count_df = df.count()
    assert count_df > 0, f"❌ {name}: DataFrame is empty"
    print(f"✅ {name}: DataFrame not empty ({count_df} rows)")

def check_no_nulls_in_critical_columns(df, cols, name):
    for c in cols:
        null_count = df.filter(col(c).isNull()).count()
        assert null_count == 0, f"❌ {name}: Column {c} has {null_count} NULL values"
    print(f"✅ {name}: No NULLs found in critical columns")

def check_unique_ids(df, unique_cols, name):
    for uc in unique_cols:
        cols_str = ", ".join(uc)
        total = df.count()
        unique = df.select([col(c) for c in uc]).distinct().count()
        assert total == unique, f"❌ {name}: Duplicate rows found for ({cols_str})"
        print(f"✅ {name}: Unique constraint passed for ({cols_str})")

def check_foreign_key_integrity(df, ref_df, fk_column, ref_column, name):
    missing = df.select(fk_column).subtract(ref_df.select(ref_column))
    count_missing = missing.count()
    assert count_missing == 0, f"❌ {name}: {count_missing} FK values in {fk_column} not found in {ref_column}"
    print(f"✅ {name}: FK integrity OK for {fk_column} → {ref_column}")

def check_range_of_values(df, rules, name):
    for r in rules:
        c, min_val, max_val = r.get('column'), r.get('min'), r.get('max', None)
        df = df.withColumn(c, col(c).cast("double"))
        cond = (col(c) < min_val) if max_val is None else ((col(c) < min_val) | (col(c) > max_val))
        out_of_range = df.filter(cond).count()
        assert out_of_range == 0, f"❌ {name}: Column {c} has {out_of_range} values out of range"
    print(f"✅ {name}: All numeric ranges OK")