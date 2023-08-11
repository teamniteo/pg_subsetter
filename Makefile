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
pgweb:is-postgres-running
	@pgweb --url "postgres://test_source@localhost:5432/test_source?sslmode=disable"
