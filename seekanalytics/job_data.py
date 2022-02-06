"""
module to hold generic job data analytics related class and code
"""
from pyspark.sql import (
    DataFrame,
    Row,
    SparkSession,
    functions as F
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StructType,
    StringType,
    StructField
)


def convert_to_list_of_dict(result: list[Row]) -> list:
    """
    convert to list of dictionary for unit testing convenience

    :param result: list of Row objects from df.collect()
    :return: list of dictionary objects
    """
    return [row.asDict(recursive=True) for row in result]


def collect_and_convert_to_list_of_dict(df: DataFrame) -> list:
    """
    collect and transform list of Row objects to list of dictionary objects
    :param df: transformed dataframe object ready to be collected
    :return: list of dictionary objects
    """
    result = df.collect()
    return convert_to_list_of_dict(result)


class JobData:
    """
    Base class to handle Job Data extract and transformations
    """
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

    def __init__(self, path: str, file_format: str = 'json') -> None:
        """
        Job data initialisation for extract and transform
        :param path: data directory / file pattern
        :param file_format: for spark.read.load format parameter
        """
        self.path = path
        self.file_format = file_format
        self.spark = SparkSession.builder.appName('sury-seek').getOrCreate()
        self.spark.sparkContext.setLogLevel('OFF')
        self._df: DataFrame = None
        self._df_jobs: DataFrame = None

    @property
    def df(self) -> DataFrame:
        # loading dataset on first use of self.df
        if self._df is None:
            self._df = self.spark.read.load(
                self.path,
                format=self.file_format,
                schema=self.schema
            )

        return self._df

    def transform_averge_salary_for_each_profile(
            self,
            df: DataFrame = None) -> DataFrame:
        """
        transform dataframe to include average salary column
        :param df: override dataframe object, defaults to self.df
        :return: transformed dataframe
        """
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

    def transform_job_with_max_salary_for_each_profile(
            self,
            df: DataFrame = None) -> DataFrame:
        """
        transform function to include job history object with maximum salary for each profile
        :param df: override dataframe object, defaults to self.df
        :return: transformed dataframe
        """
        if df is None:
            df = self.df

        init_acc = F.struct(
            F.lit(None).cast('string').alias('title'),
            F.lit(None).cast('string').alias('location'),
            F.lit(None).cast('integer').alias('salary'),
            F.lit(None).cast('string').alias('fromDate'),
            F.lit(None).cast('string').alias('toDate')
        )
        df = df.withColumn(
            'job_max',
            F.aggregate(
                'profile.jobHistory',
                init_acc,
                lambda acc, x: F.when(
                    (F.isnull(acc.salary)) | (x['salary'] >= acc.salary) & (x['fromDate'] >= acc.fromDate),
                    x
                ).otherwise(
                    acc
                ),
                lambda x: F.when(x == init_acc, None).otherwise(x)
            ),
        )
        df = df.select(
            'id',
            'profile.*',
            'job_max',
            F.col('job_max.salary').alias('max_salary'),
            F.when(F.col('job_max.fromDate') == '', 'NA').otherwise(
                F.substring('job_max.fromDate', 1, 4)
            ).alias('max_salary_year')
        )

        return df

    def transform_latest_job_for_each_profile(
            self,
            df: DataFrame = None) -> DataFrame:
        """
        transform function to add latest job as an additional column from the job history list
        :param df: override dataframe object, defaults to self.df
        :return: transformed dataframe
        """
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

    def transform_extract_all_jobs(
            self,
            df: DataFrame = None) -> DataFrame:
        """
        transform function to explode job history into multiple rows for each profile
        :param df: override dataframe object, defaults to self.df
        :return: transformed dataframe
        """
        if df is None:
            df = self.df

        if self._df_jobs is None:
            self._df_jobs = df.select(
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

        return self._df_jobs
