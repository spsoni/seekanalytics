# seekanalytics

Data analytics exercise for user job history and profile.

Detailed TODO.md is maintained in this repository.

## Build
### Docker image build:
```shell
docker build -t seekanalytics:1.0.0 .
```
or on linux like environment
```shell
make build
```

## Test
```shell
docker run --rm seekanalytics:1.0.0 pytest -v -x
```
or on linux like environment
```shell
make test
```

## Run
Please set environment variable DATADIR for input files.
And, set environment variable OUTPUTDIR for parquet output.

```shell
docker run --rm -p 4040:4040 -v $(OUTPUTDIR):/job/output -v $(DATADIR):/job/input seekanalytics:1.0.0 /job/spark-submit.sh
```
or on linux like environment
```shell
make run
```

## Sample Run Output
```shell
# Full output took 7 mins on MBP 32 GB Ram 2.9 GHz 6-Core Intel Core i9

2. Print the schema
root
 |-- id: string (nullable = true)
 |-- profile: struct (nullable = true)
 |    |-- firstName: string (nullable = true)
 |    |-- lastName: string (nullable = true)
 |    |-- jobHistory: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- title: string (nullable = true)
 |    |    |    |-- location: string (nullable = true)
 |    |    |    |-- salary: integer (nullable = true)
 |    |    |    |-- fromDate: string (nullable = true)
 |    |    |    |-- toDate: string (nullable = true)

3. How many records are there in the dataset?
17139693
4. What is the average salary for each profile?
   Display the first 10 results, ordered by lastName in descending order.
+------------------------------------+---------+--------+------------------+
|id                                  |firstName|lastName|salary_average    |
+------------------------------------+---------+--------+------------------+
|5894afab-574f-4297-bc32-faf7f5f8fdd5|Richard  |Zywiec  |69625.0           |
|82dab74c-3946-45b3-8cd1-cbdf26f49843|Robert   |Zywiec  |66833.33333333333 |
|ba24222d-6e39-40d4-a091-63dc1b0e9770|Matthew  |Zywiec  |65500.0           |
|56cc651c-0bbd-492c-91f3-f24661f6d8ba|Katie    |Zywicki |55250.0           |
|98fefdfd-d0f3-45bc-a2fd-161035bdcf98|John     |Zywicki |56666.666666666664|
|40fa57e1-5f0e-45eb-b62f-8555d7ea0dc7|James    |Zywicki |86000.0           |
|8c44a4ac-7af3-44a0-9d75-dd9e3e056e3d|Juan     |Zywicki |null              |
|f5f77def-fce9-43e5-aba4-7bddee210ea1|Viola    |Zywicki |107666.66666666667|
|296999c2-8951-4053-a94d-92b227a2664b|Christine|Zywicki |56333.333333333336|
|fd6e55be-4864-4708-8d82-bb55318c1430|Alyson   |Zywicki |null              |
+------------------------------------+---------+--------+------------------+

5. What is the average salary across the whole dataset?
97473.6229416272
6. On average, what are the top 5 paying jobs?
   Bottom 5 paying jobs? If there is a tie, please order by title, location.
6.1. Top 5:
+---------------------+---------+-----------------+
|title                |location |average_salary   |
+---------------------+---------+-----------------+
|cosmetic injector    |Melbourne|97685.17451749711|
|safety superintendent|Perth    |97670.41450212519|
|Multi Site Manager   |Melbourne|97646.6894018173 |
|trimmer              |Brisbane |97643.56431163519|
|store manager        |Hobart   |97641.65395372489|
+---------------------+---------+-----------------+

6.2. Bottom 5:
+-----------------------------------+--------+-----------------+
|title                              |location|average_salary   |
+-----------------------------------+--------+-----------------+
|Administration Officer             |Brisbane|97282.44368433245|
|business development representative|Hobart  |97314.6026583325 |
|business development representative|Canberra|97318.79203756413|
|medical radiation technologist     |Sydney  |97324.03718459496|
|medical radiation technologist     |Brisbane|97331.90110330262|
+-----------------------------------+--------+-----------------+

7. Who is currently making the most money?
   If there is a tie, please order in lastName descending, fromDate descending.
-RECORD 0-----------------------------------------
 id        | 5b217f27-8f8d-4dcb-b430-b48f14441525
 firstName | Kevin
 lastName  | Zyla
 title     | procurement specialist
 location  | Perth
 salary    | 159000
 fromDate  | 2014-07-23
 toDate    | null
 from_year | 2014

8. What was the most popular job title started in 2019?
+-----------------+----------+------------------+
|title            |jobs_count|average_salary    |
+-----------------+----------+------------------+
|Sheetmetal Worker|14764     |109220.94283392034|
+-----------------+----------+------------------+

9. How many people are currently working?

7710613
10. For each person, list only their latest job.
    Display the first 10 results, ordered by lastName descending,
    firstName ascending order.
-RECORD 0------------------------------------------------------------------------------
 id         | ba24222d-6e39-40d4-a091-63dc1b0e9770
 firstName  | Matthew
 lastName   | Zywiec
 job_latest | {Multi Site Manager, Perth, 67000, 2017-04-23, null}
-RECORD 1------------------------------------------------------------------------------
 id         | 5894afab-574f-4297-bc32-faf7f5f8fdd5
 firstName  | Richard
 lastName   | Zywiec
 job_latest | {assembler, Sydney, 83000, 2018-07-23, null}
-RECORD 2------------------------------------------------------------------------------
 id         | 82dab74c-3946-45b3-8cd1-cbdf26f49843
 firstName  | Robert
 lastName   | Zywiec
 job_latest | {registration officer, Adelaide, 85000, 2016-08-08, 2019-04-08}
-RECORD 3------------------------------------------------------------------------------
 id         | c503140a-6db4-4c8f-b75c-0e7f24e0e030
 firstName  | Albert
 lastName   | Zywicki
 job_latest | {, , 0, , }
-RECORD 4------------------------------------------------------------------------------
 id         | fd6e55be-4864-4708-8d82-bb55318c1430
 firstName  | Alyson
 lastName   | Zywicki
 job_latest | {, , 0, , }
-RECORD 5------------------------------------------------------------------------------
 id         | ff680062-9d36-41c2-a0d8-58adc8b4ae75
 firstName  | Anthony
 lastName   | Zywicki
 job_latest | {, , 0, , }
-RECORD 6------------------------------------------------------------------------------
 id         | 4e26c80a-8e84-46fc-9497-de892010cc1e
 firstName  | Bobby
 lastName   | Zywicki
 job_latest | {taxation accountant, Perth, 89000, 2017-12-11, 2019-04-11}
-RECORD 7------------------------------------------------------------------------------
 id         | f643f39c-e18a-430f-9312-650ca3c991e3
 firstName  | Calvin
 lastName   | Zywicki
 job_latest | {assistant operations manager, Adelaide, 144000, 2015-04-24, 2019-01-24}
-RECORD 8------------------------------------------------------------------------------
 id         | 03aeca24-7be1-42ab-b5a9-08e524c272fc
 firstName  | Charles
 lastName   | Zywicki
 job_latest | {sales consultant, Sydney, 95000, 2016-06-10, 2019-04-10}
-RECORD 9------------------------------------------------------------------------------
 id         | cc529ff4-2dbf-4ce1-9c7d-8f9e0edea1c6
 firstName  | Cherryl
 lastName   | Zywicki
 job_latest | {trimmer, Perth, 66000, 2017-06-01, 2019-04-01}


Question 11 and 12 output is not present here
```
