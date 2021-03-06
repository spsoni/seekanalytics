"""
Main module to answer the quest
"""
import os
from os.path import (
    abspath,
    dirname,
    join
)
from pyspark.sql import (
    DataFrame,
    functions as F
)
from pyspark.sql.types import StructType

from job_data import JobData


class JobDataAnswers(JobData):
    """
    subclass of JobData for keeping the answer functions decoupled
    from base transform functions
    """
    def answer_2(self) -> StructType:
        print('2. Print the schema')
        self.df.printSchema()
        return self.df.schema

    def answer_3(self) -> int:
        print('3. How many records are there in the dataset?')
        records = self.df.count()
        print(records)
        return records

    def answer_4(self) -> DataFrame:
        print('''4. What is the average salary for each profile? 
   Display the first 10 results, ordered by lastName in descending order.''')

        df = self.transform_averge_salary_for_each_profile()
        df = df.select('id', 'profile.*', 'salary_average')
        df = df.drop('jobHistory')
        df = df.orderBy(
            F.col('profile.lastName').desc()
        ).limit(10)
        df.show(truncate=False)
        return df

    def answer_5(self) -> int:
        print('5. What is the average salary across the whole dataset?')
        df = self.transform_extract_all_jobs()
        df = df.select(
            F.avg('salary').alias('average_salary_for_entire_dataset')
        )
        data = df.collect()[0].asDict()
        print(data['average_salary_for_entire_dataset'])
        return data['average_salary_for_entire_dataset']

    def answer_6(self) -> tuple:
        print('''6. On average, what are the top 5 paying jobs? 
   Bottom 5 paying jobs? If there is a tie, please order by title, location.''')
        df = self.transform_extract_all_jobs()
        df = df.groupby(
            'title',
            'location'
        ).agg(
            F.avg(df.salary).alias('average_salary')
        )

        print('6.1. Top 5:')
        data_top_5_df = (
            df.orderBy(
                F.col('average_salary').desc(),
                F.col('title'),
                F.col('location')
            ).limit(5)
        )
        data_top_5_df.show(truncate=False)

        print('6.2. Bottom 5:')
        data_bottom_5_df = (
            df.orderBy(
                F.col('average_salary'),
                F.col('title'),
                F.col('location')
            ).limit(5)
        )
        data_bottom_5_df.show(truncate=False)

        return data_top_5_df, data_bottom_5_df

    def answer_7(self) -> DataFrame:
        print('''7. Who is currently making the most money? 
   If there is a tie, please order in lastName descending, fromDate descending.''')

        df = self.transform_extract_all_jobs()

        # all active jobs, toDate is not set
        df = df.filter(F.isnull('toDate'))

        df = df.orderBy(
            F.col('salary').desc(),
            F.col('lastName').desc(),
            F.col('fromDate').desc()
        ).limit(1)
        df.show(truncate=False, vertical=True)
        return df

    def answer_8(self) -> DataFrame:
        print('''8. What was the most popular job title started in 2019?''')

        df = self.transform_extract_all_jobs()

        df = df.filter(df.from_year == '2019')
        df = df.groupby(
            'title'
        ).agg(
            F.count('title').alias('jobs_count'),
            F.avg('salary').alias('average_salary')
        )

        df = df.orderBy(
            F.col('jobs_count').desc(),
            F.col('average_salary').desc()
        ).limit(1)
        df.show(truncate=False)
        return df

    def answer_9(self) -> int:
        print('''9. How many people are currently working?''')
        df = self.transform_extract_all_jobs()

        # all active jobs, toDate is not set
        df = df.filter(F.isnull('toDate'))
        records = df.count()
        print(records)
        return records

    def answer_10(self) -> DataFrame:
        print('''10. For each person, list only their latest job. 
    Display the first 10 results, ordered by lastName descending, 
    firstName ascending order.''')
        df = self.transform_latest_job_for_each_profile()
        df = df.select('id', 'profile.*', 'job_latest')
        df = df.drop('jobHistory')
        df = df.orderBy(
            F.col('lastName').desc(),
            F.col('firstName')
        ).limit(10)
        df.show(truncate=False, vertical=True)
        return df

    def answer_11(self) -> DataFrame:
        print('''11. For each person, list their highest paying job along 
    with their first name, last name, salary and the year they made 
    this salary. Store the results in a dataframe, and then print 
    out 10 results''')
        df = self.transform_job_with_max_salary_for_each_profile()
        df = df.drop('jobHistory').limit(10)
        df.show(truncate=False, vertical=True)
        return df

    def answer_12(self, output_path: str) -> DataFrame:
        print('''12. Write out the last result (question 11) in parquet format, 
    compressed, partitioned by year of their highest paying job''')
        df = self.transform_job_with_max_salary_for_each_profile()
        df = df.drop('jobHistory')
        df = df.orderBy('max_salary_year', 'firstName', 'lastName')

        # actual write here
        (
            df.write
            .mode('overwrite')
            .partitionBy('max_salary_year')
            .parquet(output_path, compression='snappy')
        )
        df_parquet = self.spark.read.schema(df.schema).parquet(output_path)
        return df_parquet.orderBy('max_salary_year', 'firstName', 'lastName')

    def all_answers(self, output_path) -> None:
        """
        Combine all answer functions here
        :param output_path: path to store parquet output
        :return: None
        """
        self.answer_2()
        self.answer_3()
        self.answer_4()
        self.answer_5()
        self.answer_6()
        self.answer_7()
        self.answer_8()
        self.answer_9()
        self.answer_10()
        self.answer_11()
        self.answer_12(output_path)


def main():
    # TODO: take optparse parameters for test data and its format
    data_dir = os.environ.get(
        'DATADIR',
        join(
            dirname(dirname(abspath(__file__))),
            'input'
        )
    )
    output_dir = os.environ.get(
        'OUTPUTDIR',
        join(
            dirname(dirname(abspath(__file__))),
            'output'
        )
    )
    file_format = os.environ.get(
        'FILEFORMAT',
        'json'
    )
    # TODO: remove the need to explictly provide *.file_format here
    data_path = join(data_dir, f'*.{file_format}')

    job = JobDataAnswers(data_path, file_format=file_format)
    job.all_answers(output_dir)


if __name__ == '__main__':
    main()
