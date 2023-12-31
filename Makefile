PACKAGE=niteo.co/subsetter

.PHONY: run
run:
	go run "${PACKAGE}/$(filter-out $@,$(MAKECMDGOALS))" 

.PHONY: up
up:
	process-compose up -t=false -p=0

.PHONY: is-postgres-running
is-postgres-running:
	@(pg_isready -h localhost) || (echo "# ==> Startis postgres by running 'make up'" && exit 2)

.PHONY: pgweb
pgweb: is-postgres-running
	if [ -n "$(filter-out $@,$(MAKECMDGOALS))" ]; then \
		pgweb --url "$(filter-out $@,$(MAKECMDGOALS))"; \
	else \
		pgweb --url "postgres://test_target@localhost:5432/test_target?sslmode=disable"; \
	fi;

build:
	rm -rf dist
	goreleaser build --snapshot --clean

lint:
	golangci-lint run

dump:
	pg_dump --no-acl --schema-only -n public -x -O -c -f ./dump.sql $(filter-out $@,$(MAKECMDGOALS))

restore:
	psql -f ./dump.sql "postgres://test_target@localhost:5432/test_target?sslmode=disable"

clear:
	psql -c "DROP OWNED BY CURRENT_USER;" "postgres://test_target@localhost:5432/test_target?sslmode=disable"

test: 
	go test -timeout 30s -v ./...
