from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def convert_to_list_of_dict(result):
    # convert to list of dictionary for unit testing convenience
    return [row.asDict(recursive=True) for row in result]


class JobData:
    schema = StructType(fields=[
        StructField('id', StringType(), True),
        StructField(
            'profile',
            StructType([
                StructField('firstName', StringType(), True),
                StructField('lastName', StringType(), True),
                StructField('jobHistory', ArrayType(
                    StructType([
                        StructField('title', StringType(), True),
                        StructField('location', StringType(), True),
                        StructField('salary', IntegerType(), True),
                        StructField('fromDate', StringType(), True),
                        StructField('toDate', StringType(), True),
                    ])
                ))
            ])
        )
    ])

    def __init__(self, path, data_format: str = 'json'):
        self.path = path
        self.data_format = data_format
        self.spark = SparkSession.builder.getOrCreate()
        self._df: DataFrame = None

    @property
    def df(self) -> DataFrame:
        # loading dataset on first use of self.df
        if self._df is None:
            self._df = self.spark.read.load(self.path, format=self.data_format, schema=self.schema)

        return self._df

    def transform_averge_salary_for_each_profile(self, df: DataFrame = None) -> DataFrame:
        if df is None:
            df = self.df

        # Assumption: This is an average salary for different roles job history for an individual.
        # This is not an average income calculation here, otherwise we will have to add
        # weight (number of years) of each work experience

        # Note: intentionally avoiding complex aggregate function here to highlight
        # simplicity approach for readability and less coginitive load for fellow team members.
        # Rest of the other transform functions are using complex aggregate function, which looses
        # some readability and simplicity

        df = df.withColumn(
            'salary_sum',
            F.aggregate(
                'profile.jobHistory',
                F.lit(0),
                lambda acc, x: acc + x['salary']
            )
        ).withColumn(
            'salary_count',
            F.aggregate(
                'profile.jobHistory',
                F.lit(0),
                lambda acc, x: acc + 1
            )
        ).withColumn(
            'salary_average',
            (F.col('salary_sum') / F.col('salary_count'))
        )
        # remove unwanted columns
        df = df.drop(
            'salary_count', 'salary_sum'
        )

        return df

    def transform_job_with_max_salary_for_each_profile(self, df: DataFrame = None) -> DataFrame:
        if df is None:
            df = self.df

        df = df.withColumn(
            'job_max',
            F.aggregate(
                'profile.jobHistory',
                F.struct(
                    F.lit('').alias('title'),
                    F.lit('').alias('location'),
                    F.lit(0).alias('salary'),
                    F.lit('').alias('fromDate'),
                    F.lit('').alias('toDate')
                ),
                lambda acc, x: F.when(
                    (x['salary'] >= acc.salary) & (x['fromDate'] >= acc.fromDate),
                    x
                ).otherwise(
                    acc
                )
            )
        )

        return df

    def transform_latest_job_for_each_profile(self, df: DataFrame = None) -> DataFrame:
        if df is None:
            df = self.df

        df = df.withColumn(
            'job_latest',
            F.aggregate(
                'profile.jobHistory',
                F.struct(
                    F.lit('').alias('title'),
                    F.lit('').alias('location'),
                    F.lit(0).alias('salary'),
                    F.lit('').alias('fromDate'),
                    F.lit('').alias('toDate')
                ),
                lambda acc, x: F.when(
                    x['fromDate'] >= acc.fromDate,
                    x
                ).otherwise(
                    acc
                )
            )
        )

        return df

    def transform_extract_all_jobs(self, df: DataFrame = None) -> DataFrame:
        if df is None:
            df = self.df

        return df.select(
            'id',
            'profile.firstName',
            'profile.lastName',
            F.explode('profile.jobHistory').alias('job')
        ).select(
            'id',
            'firstName',
            'lastName',
            'job.*',
            F.substring('job.fromDate', 1, 4).alias('from_year')
        )
