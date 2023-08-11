# pg-subsetter

[![lint](https://github.com/teamniteo/pg-subsetter/actions/workflows/lint.yml/badge.svg)](https://github.com/teamniteo/pg-subsetter/actions/workflows/lint.yml) [![build](https://github.com/teamniteo/pg-subsetter/actions/workflows/go.yml/badge.svg)](https://github.com/teamniteo/pg-subsetter/actions/workflows/go.yml)


`pg-subsetter` is a powerful and efficient tool designed to synchronize a fraction of a PostgreSQL database to another PostgreSQL database on the fly, it does not copy the SCHEMA, this means that your target database has to have schema populated in some other way.

### Database Fraction Synchronization
`pg-subsetter` allows you to select and sync a specific subset of your database. Whether it's a fraction of a table or a particular dataset, you can have it replicated in another database without synchronizing the entire DB.

### Integrity Preservation with Foreign Keys
Foreign keys play a vital role in maintaining the relationships between tables. `pg-subsetter` ensures that all foreign keys are handled correctly during the synchronization process, maintaining the integrity and relationships of the data.

### Efficient COPY Method
Utilizing the native PostgreSQL COPY command, pg-subsetter performs data transfer with high efficiency. This method significantly speeds up the synchronization process, minimizing downtime and resource consumption.

### Stateless Operation
`pg-subsetter` is built to be stateless, meaning it does not maintain any internal state between runs. This ensures that each synchronization process is independent, enhancing reliability and making it easier to manage and scale.


## Usage

```
Usage of subsetter:
  -dst string
        Destination DSN
  -f float
        Fraction of rows to copy (default 0.05)
  -src string
        Source DSN
```


Example:

```pg-subsetter -src postgresql://:@/bigdb -dst postgresql://:@/littledb -f 0.05```

# Installing

See releases for downloads.
