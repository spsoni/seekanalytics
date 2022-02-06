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
- Mount volume for test data folder and file pattern to run the actual data analytics code
- No data quality checks are in place in current code, e.g. if fromDate is missing or NULL
- No unicode data validatation so far
- This solution is purely for academic purpose. In production environment, in ETL pipelines we would create intermediate datasets for option read (parquet) for regular average / min / max calculations on salaries. Basically current data can be exploded (de-normalised) for efficient data analytics


## Assumptions
- (Non-)Nullable fields are not enforced and validated
- test data files would be stored in the 
- All salaries are in integer whole numbers, but all averages calculated are decimal. I did not choose to round them off to get wrong sort orders for profiles due to rounding off


## Data Issues
- Should we exclude profiles from average calculations if they do not have work history?
