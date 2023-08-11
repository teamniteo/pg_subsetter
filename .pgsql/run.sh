#!/bin/sh
# shellcheck shell=sh disable=SC2129
set -e

# Reset the database
pg_ctl -D .pgsql/data -w stop -m fast || true
rm -rf .pgsql/data
mkdir -p .pgsql/data
pg_ctl -D .pgsql/data init;
echo "log_statement=all" >> .pgsql/data/postgresql.conf
echo "max_connections=100" >> .pgsql/data/postgresql.conf
echo "log_connections=false" >> .pgsql/data/postgresql.conf
echo "log_disconnections=false" >> .pgsql/data/postgresql.conf
echo "log_duration=true" >> .pgsql/data/postgresql.conf
echo "timezone=utc" >> .pgsql/data/postgresql.conf
echo "unix_socket_directories='/tmp'" >>.pgsql/data/postgresql.conf

# Configure the databases
pg_ctl -D .pgsql/data -w start
LANG=en_US.UTF-8 createdb -h localhost -p 5432
.pgsql/testdb.sh
pg_ctl -D .pgsql/data -w stop -m fast

# Start the server
postgres -D ./.pgsql/data
