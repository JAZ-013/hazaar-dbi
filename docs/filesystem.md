# Hazaar DBI Filesystem Backend

The Hazaar DBI library provides a DBI filesystem backend for Hazaar MVC's filesystem abstraction.  This backend can be used to
store all files and directories in any relational database supported by PDO and the Hazaar DBI library.

## Preparing the database

To use a database to store files the correct tables must exist.  Below are simple SQL scripts that can be used to create these tables.

### PostgreSQL

```sql
CREATE TABLE public.file
(
    id serial NOT NULL,
    kind text,
    parents integer[],
    filename text,
    created_on timestamp without time zone,
    modified_on timestamp without time zone,
    length integer,
    mime_type text,
    md5 text,
    owner text,
    "group" text,
    mode text,
    metadata json,
    PRIMARY KEY (id)
);

CREATE TABLE public.file_chunk
(
    id serial NOT NULL,
    file_id integer NOT NULL,
    n integer NOT NULL,
    data bytea,
    PRIMARY KEY (id),
    FOREIGN KEY (file_id)
        REFERENCES public.file (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
```

## Media Configuration

The Hazaar MVC *media.json* configuration file is used to configure media sources and needs to be configured with a new media source
that uses the `dbi` driver.

```json
{
    "sourcename": {
        "backend": "dbi",
        "options": {
            "db": {
                "driver": "psql",
                "host": "127.0.0.1",
                "dbname": "database_name",
                "user": "username",
                "password": "password"
            }
        }
    }
}
```