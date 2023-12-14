# GNU GENERAL PUBLIC LICENSE
# Version 3, 29 June 2007

# Copyright (C) 2023 CustomMade313 (https://github.com/CustomMade313)

# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.



import pandas as pd
import pyspark
from prefect import task

@task(name="Create subfile task", log_prints=True)
def create_subfile(pd: pd.DataFrame, path: str, rows: int = 100) -> None:
    pd_subset = pd.head(rows)
    pd_subset.to_csv("{0}_subset_{1}.csv".format(path, rows))

@task(name="Drop title file rows Task", log_prints=True)
def drop_rows(pd: pd.DataFrame, columns: list) -> pd.DataFrame:
    """ Drop null values defined in columns list provided then return
        the new pandas DataFrame"""
    pd = pd.dropna(subset=columns)
    return pd
 
@task(name="Get Spark DataFrame schema Task", log_prints=True)
def get_df_schema(df: pyspark.sql.DataFrame) -> str:
    return df.schema

@task(name="Transform a pandas DataFrame to a Spark DataFrame Task", log_prints=True)
def pd_to_df(pd: pd.DataFrame, spark_session: pyspark.sql.session.SparkSession) -> pyspark.sql.DataFrame:
    """ Transform a pandas DataFrame to a Spark DataFrame """
    return spark_session.createDataFrame(pd)

@task(name="Write a Spark Dataframe schema to a file Task", log_prints=True)
def write_schema_to_file(df: pyspark.sql.DataFrame, path: str) -> None:
    schema = get_df_schema(df)
    with open(path, 'w') as f:
        print(schema)
        f.write(str(schema))

@task(name="Read a Spark Dataframe schema to a file Task", log_prints=True)
def get_schema_from_file(path:str) -> str:
    schema = ''
    with open(path, 'r') as f:
        schema = f.read()
    return schema

@task(name="Create a pandas DataFrame from a CSV file Task", log_prints=True)
def pd_from_csv(path: str) -> pd.DataFrame:
    """ Read a csv file and return a pandas DataFrame """
    return pd.read_csv(path)

@task(name="Save a pandas DataFrame to a Parquet file Task", log_prints=True)
def pd_to_parquet(pd: pd.DataFrame, path: str) -> None:
    """ Write pandas DataFrame to path as a parquet file """
    pd.to_parquet(path)

@task(name="Save a pandas Dataframe to a CSV file Task", log_prints=True)
def pd_to_csv(pd: pd.DataFrame, path: str) -> None:
    """ Save a pandas DataFrame to csv """
    pd.to_csv(path, index=False)
    
