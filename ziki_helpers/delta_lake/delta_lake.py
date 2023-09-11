from delta.tables import DeltaTable

def upsert(df, table_path, spark, key_col='guid'):
    delta_table = DeltaTable.forPath(spark, table_path)

    delta_table.alias("old_df").merge(
        df.alias("new_df"),
        f"old_df.{key_col} = new_df.{key_col}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
