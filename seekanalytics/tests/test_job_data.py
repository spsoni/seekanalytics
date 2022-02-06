from os.path import dirname, abspath, join

import pytest

from job_data import JobData, DataFrame, convert_to_list_of_dict, F

PROJECT_DIR = dirname(dirname(abspath(__file__)))
TEST_DATA = join(PROJECT_DIR, 'data')


@pytest.fixture
def data_job():
    return JobData(path=TEST_DATA)


def test_init(data_job):
    assert isinstance(data_job, JobData)
    assert isinstance(data_job.df, DataFrame)
    assert data_job.schema == data_job.df.schema
    assert data_job.df.count() == 20


def test_transform_averge_salary_for_each_profile(data_job):
    df = data_job.transform_averge_salary_for_each_profile()
    data = convert_to_list_of_dict(df.collect())

    assert len(data) == 20

    assert data[1]['id'] == 'a4c6238d-0aed-4eb8-b60c-242983e43edb'
    assert data[1]['salary_average'] == 135625.0
    assert len(data[1]['profile']['jobHistory']) == 8

    salary_average_list = [row['salary_average'] for row in data if row['salary_average']]
    overall_salary_average = sum(salary_average_list) / 20.0
    assert overall_salary_average == 82917.24206349206

    # now test again with even smaller dataset by overriding df parameter of the transform fn
    small_df = data_job.df.filter(F.col('profile.firstName').startswith('S'))
    df = data_job.transform_averge_salary_for_each_profile(df=small_df)
    data = convert_to_list_of_dict(df.collect())

    assert len(data) == 2

    assert data[1]['id'] == '7156c3d0-086a-4f6a-af54-09441d2d0026'
    assert data[1]['salary_average'] is None
    assert len(data[1]['profile']['jobHistory']) == 0

    salary_average_list = [row['salary_average'] for row in data if row['salary_average']]
    overall_salary_average = sum(salary_average_list) / 2.0
    assert overall_salary_average == 35214.28571428572


# keeping remaining tests short, detailed intention is highlighted in previous test case
def test_transform_job_with_max_salary_for_each_profile(data_job):
    df = data_job.transform_job_with_max_salary_for_each_profile()
    data = convert_to_list_of_dict(df.collect())

    assert len(data) == 20

    assert data[2]['id'] == 'dcbae85f-4971-4fdc-a8a9-dacd7fde49fc'
    assert data[2]['job_max']['title'] == 'procurement specialist'
    assert data[2]['job_max']['salary'] == 131000

    assert data[8]['id'] == 'dbf4c94f-a5ab-4661-a5ce-1928d9dccd9c'
    assert data[8]['job_max']['title'] == 'corporate consultant'
    assert data[8]['job_max']['salary'] == 73000


def test_transform_latest_job_for_each_profile(data_job):
    df = data_job.transform_latest_job_for_each_profile()
    data = convert_to_list_of_dict(df.collect())

    assert len(data) == 20

    assert data[4]['id'] == '915730cc-3947-4b00-a80b-091f912350b1'
    assert data[4]['job_latest']['title'] == 'hr advisor'
    assert data[4]['job_latest']['salary'] == 66000


def test_transform_extract_all_jobs(data_job):
    df = data_job.transform_extract_all_jobs()
    data = convert_to_list_of_dict(df.collect())

    assert len(data) == 89

    assert data[7]['id'] == 'a4c6238d-0aed-4eb8-b60c-242983e43edb'
    assert data[7]['title'] == 'procurement specialist'
    assert data[7]['salary'] == 123000
    assert data[7]['location'] == 'Hobart'
