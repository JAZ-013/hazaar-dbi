# Hazaar DBI Documentation

## Bootstrap

In your application configuration directory you need to create a database.ini file which looks a bit like this:

```ini
db.driver = {driver anme}
db.host = {your db host}
db.user = {your db user}
db.password = {password}
db.dbname = {dbname}
db.master = {your master db host} #Optional.  Use only with PGSQL replication.  See below.
```

Drivers are the PDO driver name, such as pgsql, mysql, etc.

In your bootstrap, add:

```php
Hazaar\Db\Adapter::configure($this->config->db);
```

## Using the adapter

```php
$db = new Hazaar\Db\Adapter();   
$select = new Hazaar\Db\Select('*', 'users');
$result = $db->select($select);
while($row = $result->fetch()){
    //Do things with $row here
}
```

## PostgreSQL Replication

If your database host is running PostgreSQL 9.0+ replication then Hazaar has some extra magic for you. It's possible to use a read-only slave for most queries and then have Hazaar's database adapter automatically send all write operations to the master. Without the application knowing, or caring.

To achieve this, all you need to do is add the

```ini
db.master
```

parameter to the database.ini file and the Hazaar DB adapter will take care of the rest.

### How does it work?

Basically, if the `db.master` parameter is set, then the adapter knows to check if the `db.host` is a slave by executing thePGSQL specific query:

```sql
SELECT pg_is_in_recovery()
```

This query indicates that the host is in recovery mode, meaning it is a replication slave. If this is true, then the adapter will create a second connection using the main connection parameters but switches out the host parameter with the value in `db.master`.

After that, any write operations will use the second connection which will write to the master.