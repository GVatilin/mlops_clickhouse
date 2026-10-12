.PHONY: run script query

CSV_PATH ?= data/train.csv
RESULT_PATH ?= results/top_category_by_state.csv

run:
	docker compose up -d --build

script:
	@test -f "$(CSV_PATH)" || { echo "Файл не найден: $(CSV_PATH)"; exit 1; }
	docker compose run --rm --build -v "$(abspath $(CSV_PATH)):/input.csv:ro" producer --path /input.csv

query:
	docker compose exec -T clickhouse clickhouse-client --time --format CSVWithNames < sql/top_category_by_state.sql > $(RESULT_PATH)
