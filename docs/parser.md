# SQL SELECT Parser

## What is it?

There may be a time in your application where you want to read and modify an SQL query.  An example of this may be allowing users to write
queries, but then processing these queries in your application before passing them on to the database server for execution.  You may want
to override certain parameters.  Or implement some sort of table-based security.  This feature will allow you to parse the SQL and returns
a `Hazaar\DBI\Table` object that can optionally be updated before executing the query.

!!! notice
    The SQL parser will only work with SELECT queries.  INSERT, UPDATE, DELETE and other queries are not supported.

## Using the Parser

Creating a new parser is available from the `Hazaar\DBI\Adapter` class by calling the `parseSQL()` method and passing it the SQL string.

```php
$db = new \Hazaar\DBI\Adapter();

$sql = 'SELECT id, name, t.name as type_name FROM my_records r INNER JOIN my_record_type t ON r.type_id=t.id WHERE r.id=1'

$query = $db->parseSQL($sql);
```

The `$query` variable now references a `Hazaar\DBI\Table` object just as calling `Hazaar\DBI\Adapter::table()` would.  You can then execute
the query and fetch rows immediately if you would like.

```php
while($row = $query->fetch())
    print_r($row); //Process your row here
```

If you would like to, you are able to modify the query using the convenience of Hazaar DBI programatic queries as you would normally.  For example, to add sorting you can.

```php
$query->sort('id');
```

You could even add a join if you need to.

```php
$query->join('another_table', array('r.ref_id' => array('$ref' => 'a.id')), 'a');
```

And that's really all there is to it.  See some of the example below for a bit more of an idea what is possible.

## Examples

### Overriding Query Parameters

This is probably the most common use of the SQL parser.  That is making sure that OFFSET and LIMIT are overridden correctly without the need for complicated RegEx expressions.

```php
$db = new \Hazaar\DBI\Adapter();

$sql = 'SELECT name, t.name as type_name FROM my_records'

$query = $db->parseSQL($sql)->offset(200)->limit(100);
```

This will generate the SQL: 

```SQL
SELECT name, t.name as type_name FROM my_records OFFSET 200 LIMIT 100;
```

You can also ensure that a particular column is included in the select.

```php
$db = new \Hazaar\DBI\Adapter();

$sql = 'SELECT name, t.name as type_name FROM my_records'

$query = $db->parseSQL($sql)->select('id');
```

This will generate the SQL: 

```SQL
SELECT name, t.name as type_name, id FROM my_records;
```


### Restricting Access to Tables

```php
$allowed_tables = array('my_records', 'my_record_type');

$db = new \Hazaar\DBI\Adapter();

$sql = 'SELECT id, name, t.name as type_name FROM my_records r INNER JOIN my_record_type t ON r.type_id=t.id WHERE r.id=1'

$query = $db->parseSQL($sql);

foreach($query->listUsedTables() as $table){

    if(!in_array($table, $allowed_tables))
        throw new \Exception("You are not allowed to access table '$table'!");

}
```