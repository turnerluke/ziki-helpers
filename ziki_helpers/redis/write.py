
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import redis


def dataframe_to_redis(r: redis.Redis, df: pd.DataFrame, key: str, expire: int = None) -> None:
    table = pa.Table.from_pandas(df)
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)

    parquet_bytes = buffer.getvalue().to_pybytes()
    if expire is None:
        r.set(key, parquet_bytes)
    else:
        r.setex(key, expire, parquet_bytes)