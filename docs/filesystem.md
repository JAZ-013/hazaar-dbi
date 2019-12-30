# Hazaar DBI Filesystem Backend

The Hazaar DBI library provides a DBI filesystem backend for Hazaar MVC's filesystem abstraction.  This backend can be used to
store all files and directories in any relational database supported by PDO and the Hazaar DBI library.

## Preparing the database

To use a database to store files the correct tables must exist.  Below are simple SQL scripts that can be used to create these tables.

### PostgreSQL

```sql
CREATE TABLE public.hz_file_chunk
(
    id serial NOT NULL,
    parent integer,
    n integer NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (parent)
        REFERENCES public.hz_file_chunk (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE INDEX hz_file_chunk_parent_idx
    ON public.hz_file_chunk USING btree
    (parent ASC NULLS LAST);

CREATE TABLE public.hz_file
(
    id serial NOT NULL,
    kind text,
    parent integer,
    start_chunk integer,
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
    PRIMARY KEY (id),
    FOREIGN KEY (start_chunk)
        REFERENCES public.hz_file_chunk (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE INDEX hz_file_parent_idx
    ON public.hz_file USING btree
    (parent ASC NULLS LAST);
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