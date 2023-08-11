#!/bin/sh
# shellcheck shell=sh
set -e


psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc "postgresql://$POSTGRES_USER@localhost:5432/$POSTGRES_DB" <<-EOSQL
  CREATE USER test_source WITH PASSWORD '';
  CREATE DATABASE test_source;
  GRANT ALL PRIVILEGES ON DATABASE test_source TO test_source;
  ALTER DATABASE test_source OWNER TO test_source;
EOSQL

psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc "postgresql://$POSTGRES_USER@localhost:5432/$POSTGRES_DB" <<-EOSQL
  CREATE USER test_target WITH PASSWORD '';
  CREATE DATABASE test_target;
  GRANT ALL PRIVILEGES ON DATABASE test_target TO test_target;
  ALTER DATABASE test_target OWNER TO test_target;
EOSQL