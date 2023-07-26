
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import redis


def dataframe_from_redis(r: redis.Redis, key: str) -> pd.DataFrame:
    try:
        parquet_bytes = r.get(key)
        buffer = pa.BufferReader(parquet_bytes)
        table = pq.read_table(buffer)
        df = table.to_pandas()
        return df

    except:
        return None


def get_keys_by_prefix(r: redis.Redis, prefix: str) -> list[str]:
    keys = r.keys(f'{prefix}*')
    return keys


if __name__ == '__main__':
    r = redis.Redis()
    print(dataframe_from_redis(r, 'test'))