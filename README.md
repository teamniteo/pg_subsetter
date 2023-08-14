# pg-subsetter

[![lint](https://github.com/teamniteo/pg-subsetter/actions/workflows/lint.yml/badge.svg)](https://github.com/teamniteo/pg-subsetter/actions/workflows/lint.yml) [![build](https://github.com/teamniteo/pg-subsetter/actions/workflows/go.yml/badge.svg)](https://github.com/teamniteo/pg-subsetter/actions/workflows/go.yml) [![vuln](https://github.com/teamniteo/pg-subsetter/actions/workflows/vuln.yml/badge.svg)](https://github.com/teamniteo/pg-subsetter/actions/workflows/vuln.yml)


`pg-subsetter` is a tool designed to synchronize a fraction of a PostgreSQL database to another PostgreSQL database on the fly, it does not copy the SCHEMA. 


### Database Fraction Synchronization
`pg-subsetter` allows you to select and sync a specific subset of your database. Whether it's a fraction of a table or a particular dataset, you can have it replicated in another database without synchronizing the entire DB.

### Integrity Preservation with Foreign Keys
Foreign keys play a vital role in maintaining the relationships between tables. `pg-subsetter` ensures that all foreign keys(one-to-one, one-to many, many-to-many) are handled correctly during the synchronization process, maintaining the integrity and relationships of the data. 

### Efficient COPY Method
Utilizing the native PostgreSQL COPY command, `pg-subsetter` performs data transfer with high efficiency. This method significantly speeds up the synchronization process, minimizing downtime and resource consumption.

### Stateless Operation
`pg-subsetter` is built to be stateless, meaning it does not maintain any internal state between runs. This ensures that each synchronization process is independent, enhancing reliability and making it easier to manage and scale.

### Sync required rows
`pg-subsetter` can be instructed to copy certain rows in specific tables, the command can be used multiple times to sync more data.

## Usage

```
Usage of subsetter:
  -dst string
    	Destination database DSN
  -exclude value
    	Query to ignore tables 'users: id = 1', can be used multiple times
  -f float
    	Fraction of rows to copy (default 0.05)
  -include value
    	Query to copy required tables 'users: id = 1', can be used multiple times
  -src string
    	Source database DSN
  -v	Release information
  -verbose
    	Show more information during sync (default true)
```


### Example


Prepare schema in target database:

```bash
pg_dump --schema-only -n public -f schemadump.sql "postgres://test_source@localhost:5432/test_source?sslmode=disable"
psql -f schemadump.sql "postgres://test_target@localhost:5432/test_target?sslmode=disable"
```

Copy a fraction of the database and force certain rows to be also copied over:

```
pg-subsetter \
      -src "postgres://test_source@localhost:5432/test_source?sslmode=disable" \
      -dst "postgres://test_target@localhost:5432/test_target?sslmode=disable" \
      -f 0.5
      -include "user: id=1"
      -include "group: id=1"
      -exclude "domains: domain_name ilike '%.si'"

```

# Installing

```bash
curl -Ls https://github.com/teamniteo/pg-subsetter/releases/latest/download/pg-subsetter_Linux_x86_64.tar.gz | tar -xz && mv pg-subsetter /usr/bin
```

For other downloads see releases.
