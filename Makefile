DATADIR="${DATADIR:-$PWD}"
FILEFORMAT="${FILEFORMAT:-json}"
VERSION="1.0.0"

build:
	docker build . -t seekanalytics:$(VERSION)

test:
	docker run --rm seekanalytics:$(VERSION) pytest -v -x

run:
	docker run --rm -p 4040:4040 -v $(DATADIR):/job/test_data \
		seekanalytics:$(VERSION) /job/spark-submit.sh
