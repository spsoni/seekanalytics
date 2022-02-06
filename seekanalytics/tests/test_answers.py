from os.path import dirname, abspath, join

import pytest

from job_data import collect_and_convert_to_list_of_dict
from main import JobDataAnswers

PROJECT_DIR = dirname(dirname(abspath(__file__)))
TEST_DATA = join(PROJECT_DIR, 'data')


@pytest.fixture
def data_job():
    return JobDataAnswers(path=TEST_DATA, file_format='json')


def test_answer2(data_job):
    result = data_job.answer_2()
    assert result == data_job.schema


def test_answer3(data_job):
    result = data_job.answer_3()
    assert result == 20


def test_answer4(data_job):
    result = data_job.answer_4()
    expected_schema = 'StructType(List(' \
                      'StructField(id,StringType,true),' \
                      'StructField(firstName,StringType,true),' \
                      'StructField(lastName,StringType,true),' \
                      'StructField(salary_average,DoubleType,true)))'
    assert str(result.schema) == expected_schema
    data = result.collect()
    row_0 = {
        'id': 'b9639e56-8ee6-4531-b2e2-a9add5eb67da',
        'firstName': 'Stuart',
        'lastName': 'Zumwalt',
        'salary_average': 70428.57142857143
    }
    assert data[0].asDict() == row_0
    row_last = {
        'id': '8ed452de-71c1-43ff-b3b5-13159913808b',
        'firstName': 'Myrna',
        'lastName': 'Meuse',
        'salary_average': 101000.0
    }
    assert data[-1].asDict() == row_last
    assert len(data) == 10


def test_answer5(data_job):
    result = data_job.answer_5()
    assert result == 88516.85393258427


def test_answer6(data_job):
    top5_df, bottom5_df = data_job.answer_6()

    top5 = collect_and_convert_to_list_of_dict(top5_df)
    bottom5 = collect_and_convert_to_list_of_dict(bottom5_df)

    top5_expected = [
        {'title': 'Administration Officer', 'location': 'Hobart', 'average_salary': 148000.0},
        {'title': 'customer service officer', 'location': 'Hobart', 'average_salary': 148000.0},
        {'title': 'sales representative', 'location': 'Hobart', 'average_salary': 148000.0},
        {'title': 'medical radiation technologist', 'location': 'Hobart', 'average_salary': 143000.0},
        {'title': 'clinical psychologist', 'location': 'Brisbane', 'average_salary': 142000.0}
    ]
    assert len(top5) == 5
    assert top5 == top5_expected

    bottom5_expected = [
        {'title': 'Support Analyst', 'location': 'Canberra', 'average_salary': 20000.0},
        {'title': 'property manager', 'location': 'Canberra', 'average_salary': 29000.0},
        {'title': 'principal', 'location': 'Canberra', 'average_salary': 30000.0},
        {'title': 'audit', 'location': 'Canberra', 'average_salary': 46000.0},
        {'title': 'registration officer', 'location': 'Sydney', 'average_salary': 46000.0}
    ]
    assert len(bottom5) == 5
    assert bottom5 == bottom5_expected


def test_answer7(data_job):
    df = data_job.answer_7()
    result = collect_and_convert_to_list_of_dict(df)[0]
    result_expected = {
        'id': '41a6f59c-345e-445c-ae04-599db08731db',
        'firstName': 'William',
        'lastName': 'Avalos',
        'title': 'customer service officer',
        'location': 'Brisbane',
        'salary': 124000,
        'fromDate': '2017-01-23',
        'toDate': None,
        'from_year': '2017'
    }
    assert result == result_expected


def test_answer8(data_job):
    df = data_job.answer_8()
    result = collect_and_convert_to_list_of_dict(df)[0]
    result_expected = {'title': 'technician', 'jobs_count': 1, 'average_salary': 86000.0}
    assert result == result_expected


def test_answer9(data_job):
    result = data_job.answer_9()
    result_expected = 4
    assert result == result_expected


def test_answer10(data_job):
    df = data_job.answer_10()
    result = collect_and_convert_to_list_of_dict(df)
    assert len(result) == 10
    result_expected = [
        {'id': 'b9639e56-8ee6-4531-b2e2-a9add5eb67da', 'firstName': 'Stuart', 'lastName': 'Zumwalt',
         'job_latest': {'title': 'technician', 'location': 'Perth', 'salary': 86000, 'fromDate': '2019-03-23', 'toDate': None}},
        {'id': '8ed452de-71c1-43ff-b3b5-13159913808b', 'firstName': 'Myrna', 'lastName': 'Meuse',
         'job_latest': {'title': 'technician', 'location': 'Melbourne', 'salary': 113000, 'fromDate': '2017-08-08', 'toDate': '2019-03-08'}}
    ]
    assert result[0] == result_expected[0]
    assert result[-1] == result_expected[-1]


def test_answer11(data_job):
    df = data_job.answer_11()
    result = collect_and_convert_to_list_of_dict(df)
    result_expected = [
        {'id': 'e23c7ab2-6479-4014-9baf-15d0e5191dc9', 'firstName': 'Elizabeth', 'lastName': 'Robledo',
         'job_max': {'title': 'middleware specialist', 'location': 'Perth', 'salary': 116000, 'fromDate': '2013-03-13', 'toDate': '2019-02-13'}},
        {'id': 'f331d115-95a5-4a14-845b-a052bcd71c4c', 'firstName': 'Ferne', 'lastName': 'Ott',
         'job_max': {'title': 'middleware specialist', 'location': 'Hobart', 'salary': 69000, 'fromDate': '2014-04-13', 'toDate': '2019-03-13'}}
    ]

    assert len(result) == 10
    assert result[0] == result_expected[0]
    assert result[-1] == result_expected[-1]
