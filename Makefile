VERSION="1.0.0"

build:
	docker build . -t seekanalytics:$(VERSION)

test:
	docker run --rm seekanalytics:$(VERSION) pytest -v -x

check-env-input:
ifndef DATADIR
	$(error Please set environment variable DATADIR for input files)
endif


check-env-output:
ifndef OUTPUTDIR
	$(error Please set environment variable OUTPUTDIR for parquet output)
endif

run: check-env-output check-env-input
	docker run --rm -p 4040:4040 -v $(OUTPUTDIR):/job/output -v $(DATADIR):/job/input \
		seekanalytics:$(VERSION) /job/spark-submit.sh
