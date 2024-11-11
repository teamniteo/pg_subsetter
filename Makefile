PACKAGE=niteo.co/subsetter

.PHONY: run
run:
	go run ./cli -src "postgres://test_source@localhost:5432/test_source?sslmode=disable" -dst "postgres://test_target@localhost:5432/test_target?sslmode=disable" \
        -f 0.5 \
        --include "users:id='fd7e087d-67cf-4f05-902e-29ec6212f412'" \
        --exclude domains \
		--exclude domains_godaddy \
		--exclude domains_whoisfreaks \
		--exclude domains_dropcatch \
		--exclude domains_namesilo \
		--exclude domains_sedo \
		--exclude domains_namecheap \
		--exclude domains_snapnames


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
