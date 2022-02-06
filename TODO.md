# TODO
Lots of potential improvements can be done in this code.

## Enhancements
- Improved (future-proof) Dockerfile with parameters for Java and PySpark versions
- Makefile for most of the common activities like build, test and run
- CLI parameters and environment variable based generalisation for any assumptions made in this code
- Exception handling for any bad data record or files
- Spark Configuration fine-tuning according to the dataset
- parameterised pytests for more comprehensive testing with multiple test data files
- CI/CD action script for automatic Docker build
- No data quality checks are in place in current code, e.g. if fromDate is missing or NULL
- No unicode data validatation so far
- This solution is purely for academic purpose. In production environment, in ETL pipelines we would create intermediate datasets for read performance (parquet) for regular average / min / max calculations on salaries. Basically current data can be exploded (de-normalised) and stored in parquet format for efficient data analytics
- store release number in one file and use it various build files
- add changelog.md file for keeping track of all enhancements in newer versions

## Assumptions
- (Non-)Nullable fields are not enforced and validated
- test data file folder would be shared to docker container path /job/test_data as mount volume -v ${local_path}:/job/test_data 


## Data Issues
- Should we exclude profiles from average calculations if they do not have any work history?
