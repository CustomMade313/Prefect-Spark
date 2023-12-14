# GNU GENERAL PUBLIC LICENSE
# Version 3, 29 June 2007

# Copyright (C) 2023 CustomMade313 (https://github.com/CustomMade313)

# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

from prefect import flow, task
from utils import drop_rows, pd_from_csv, pd_to_csv
from pyspark.sql import types, SparkSession
import pyspark





@flow(name='Main Flow',
      description='The entrypoint of executing all the other sub-flows'     
)
def main_flow(titles_name: str, credits_name: str, raw_path: str, pq_path: str, pq_titles_subdir: str, pq_credits_subdir: str) -> None: 
    new_titles_name = clean_titles_file_flow(titles_name)
    turn_csv_to_parquet_flow(raw_path, pq_path, pq_titles_subdir, pq_credits_subdir, new_titles_name, credits_name)
    execute_queries_flow(pq_path, pq_titles_subdir, pq_credits_subdir)

@flow(name='CSV to Parquet Flow',
      description='A Flow that transforms CSV files to Parquet' 
)
def turn_csv_to_parquet_flow(raw_path: str, pq_path: str, pq_titles_subdir: str, 
                             pq_credits_subdir: str, titles_name: str, credits_name: str) -> None:

    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName('Turn tiles and credits csv files to parquet')\
        .getOrCreate()

    credits_schema = types.StructType([
        types.StructField('person_id', types.IntegerType(), True),
        types.StructField('id', types.StringType(), True),
        types.StructField('name', types.StringType(), True),
        types.StructField('character', types.StringType(), True),
        types.StructField('role', types.StringType(), True)
    ])

    titles_schema = types.StructType([
        types.StructField('id', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('type', types.StringType(), True),
        types.StructField('description', types.StringType(),True),
        types.StructField('release_year', types.IntegerType(), True),
        types.StructField('age_certification', types.StringType(), True),
        types.StructField('runtime', types.LongType(), True),
        types.StructField('genres', types.StringType(), True),
        types.StructField('production_countries', types.StringType(), True),
        types.StructField('seasons', types.IntegerType(), True),
        types.StructField('imdb_id', types.StringType(), True),
        types.StructField('imdb_score', types.DoubleType(), True),
        types.StructField('imdb_votes', types.IntegerType(), True),
        types.StructField('tmdb_popularity', types.DoubleType(), True),
        types.StructField('tmdb_score', types.DoubleType(), True)
    ])

    df_titles = spark_session.read \
                .option('header', 'true') \
                .schema(titles_schema) \
                .csv(f"{raw_path}{titles_name}.csv")
    
    df_credits = spark_session.read \
                .option('header', 'true') \
                .schema(credits_schema) \
                .csv(f"{raw_path}{credits_name}.csv")
    

    df_titles.write.option("header", "true").mode("overwrite").parquet(f"{pq_path}{pq_titles_subdir}")

    df_credits.write.mode("overwrite").parquet(f"{pq_path}{pq_credits_subdir}")


@flow(name='Clean Titles file Flow',
      description='A Flow that drops title file rows that contain null values \
      in certain columns'
)
def clean_titles_file_flow(titles_name: str) -> str:
    pd_titles = pd_from_csv('./data/raw/{0}.csv'.format(titles_name))
    title_columns = ['production_countries', 'tmdb_score', 'imdb_score', 'genres']
    transformed_titles_pd = drop_rows(pd_titles, title_columns)
    new_name = 'titles_transformed'
    pd_to_csv(transformed_titles_pd, './data/raw/{0}.csv'.format(new_name))
    return new_name


@task(name='Join two Spark DataFrames on a give key Task')
def join_operation(df1: pyspark.sql.DataFrame, df2: pyspark.sql.DataFrame, key: str)  -> pyspark.sql.DataFrame:
    return df1.join(df2, on=key)

@flow(name='Execute Join Operation between titles and credits Spark DataFrames flow')
def join_operation_flow(df_titles, df_credits, key) -> pyspark.sql.DataFrame:

    df_titles.createOrReplaceTempView('titles')

    df_credits.createOrReplaceTempView('credits')

    df_joined_data = join_operation(df_titles, df_credits, key)

    return df_joined_data



@flow(name='Execute queries Flow',
      description='A Flow that contains all the query execution sub-flows'
)
def execute_queries_flow(pq_path: str, pq_titles_subdir: str, pq_credits_subdir: str) -> None:

    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName('Main work - execute queries')\
        .getOrCreate()
    
    df_titles = spark_session.read.parquet(f"{pq_path}{pq_titles_subdir}")

    df_credits = spark_session.read.parquet(f"{pq_path}{pq_credits_subdir}")

    df_joined_data = join_operation_flow(df_titles, df_credits, 'id')

    df_joined_data.show(10)


if __name__ == '__main__':
    
    titles_name = 'titles'
    credits_name = 'credits'
    raw_path = './data/raw/'
    pq_path = './data/pq/'
    pq_titles_subdir = 'titles'
    pq_credits_subdir = 'credits'
    
    main_flow(titles_name, credits_name, raw_path, pq_path, pq_titles_subdir, pq_credits_subdir)