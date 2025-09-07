.PHONY: all go-test py-test build cli help clean

all: go-test py-test build

go-test:
	go test ./...

py-test:
	pytest -q

build:
	go build -o ampybusctl ./cmd/ampybusctl

clean:
	rm -f ampybusctl

help:
	@echo "Available targets:"
	@echo "  all      - Run all tests and build CLI (default)"
	@echo "  go-test  - Run Go tests"
	@echo "  py-test  - Run Python tests"
	@echo "  build    - Build CLI tool"
	@echo "  clean    - Remove built binaries"
	@echo "  help     - Show this help message"

cli:
	go build -o ampybusctl ./cmd/ampybusctl

replayer:
	go build -o replayer ./examples/go/replayer

run-replayer: replayer
	./replayer

sub-bars: cli
	./ampybusctl sub --subject "ampy.prod.bars.v1.>"

.PHONY: dlq-inspect dlq-redrive dlq-poison
TLS_ARGS=--nats tls://127.0.0.1:4222 --tls-ca $(HOME)/ampybus-tls/ca.pem --tls-cert $(HOME)/ampybus-tls/client-cert.pem --tls-key $(HOME)/ampybus-tls/client-key.pem

dlq-poison:
	python3 python/examples/py_send_poison.py $(TLS_ARGS)

dlq-inspect:
	python3 python/examples/py_dlq_inspect.py --subject "ampy.prod.dlq.v1.>" --max 5 --decode --outdir ./dlq_dump $(TLS_ARGS)

dlq-redrive:
	python3 python/examples/py_dlq_redrive.py --subject "ampy.prod.dlq.v1.>" --max 5 $(TLS_ARGS)

.PHONY: kafka-up kafka-sub kafka-pub
KAFKA_BROKERS=127.0.0.1:9092
TOPIC=ampy.prod.bars.v1.XNAS.AAPL

kafka-up:
	docker run -d --name redpanda -p 9092:9092 -p 9644:9644 docker.redpanda.com/redpanda/redpanda:latest \
		redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false \
		--kafka-addr "PLAINTEXT://0.0.0.0:9092" --advertise-kafka-addr "PLAINTEXT://127.0.0.1:9092" || true

kafka-sub: ./kafkabusctl
	./kafkabusctl sub --brokers $(KAFKA_BROKERS) --topic $(TOPIC) --group cli-consumer

kafka-pub: ./kafkabusctl
	./kafkabusctl ensure-topic --brokers $(KAFKA_BROKERS) --topic $(TOPIC) --partitions 3
	./kafkabusctl pub-empty --brokers $(KAFKA_BROKERS) --topic $(TOPIC) --producer yfinance-go@ingest-1 --source yfinance-go --pk XNAS.AAPL

.PHONY: bench-kafka bench-nats
KAFKA_BROKERS=127.0.0.1:9092
SUBJ=ampy.prod.bars.v1.XNAS.AAPL

bench-kafka: ./benchkafka
	./benchkafka --brokers $(KAFKA_BROKERS) --topic $(SUBJ) --partitions 3 \
		--count 3000 --concurrency 6 --payload-bytes 512 --gzip --timeout 40s \
		--slo-p95-ms 150 --slo-p99-ms 300 --min-throughput 400

bench-nats: ./benchnats
	./benchnats --subject $(SUBJ) --nats nats://127.0.0.1:4222 \
		--count 3000 --concurrency 6 --payload-bytes 512 --gzip --timeout 40s \
		--slo-p95-ms 150 --slo-p99-ms 300 --min-throughput 400
