# Hazaar DBI Data Synchronisation

## Data Sync File Format

Datasync files are text files with JSON formatted objects.  The main container of a datasync file is a JSON array.  This allows synchronisations to be rolled out in stages as each element in the container array is processed in order. This is especially useful in relational databases where you need to link records.

An example layout could be:

```json
[                                               //Sync Container Array
    {                                           //Sync Stage Object
        "table": "{{tablename}}",                 //The name of the table this element will work with.
        "insertonly": false,                    //Will only insert new records.  Useful for system initialisation.
        "keys": [ "optional" ],                 //Optional list of fields to use as the unique identifier keys (see Unique Identifier Keys).
        "rows": [                               //An array of row object elements that will be sync'd into the database. 
            {
                "field1": "value1",             //String fields are supported.
                "field2": 2,                    //Numeric fields, including floats/doubles, are supported.
                "field3": true,                 //Boolean fields are supported.
                "field4": [ "a", "b", "c" ],    //Native [ARRAY] columns are also supported.
                "field5": {                     //JSON Objects are also supported if the target column has a JSON data type.  These fields also support
                    "key1": "element1",         //Array objects which are defined the same as the above [ARRAY] data type fields.  The data sync engine
                    "key2": "element2"          //will detect that target column type and convert the field value as needed.
                }
            }
        ]
    }
]
```

Datasync files are used to make sure that required data exists in the database.  This can be for an initial installation that will create records in a database automatically as part of system initialisation.  These can also be used to ensure that any data that is referenced in code actually exists in the database.  Normally this is done to define types of things that have an identifier that will be referenced in code but linked to other entities in the database.

### Existing Row Detection

The idea behind the data sync engine is to ensure that a row, as it is defined in the data sync file, exists in the database.  How existing rows are identified by the sync engine depends on the fields defined in the row objects as well as how the sync stage object is defined.

There are three operating modes described below in the order in which they are prioritised.

* *Primary key* - The row object has had the primary key value defined.
* *Key List* - A list of field names is defined whose values will be used to find existing records.
* *Object* - The entire object is used to find existing records.

!!! warning
    It is possible to mix and match only *Primary Key* and *Object* modes as neither of these modes require keys to be defined.  However this is not recommended.

#### Primary Key Mode

This is the fastest and most reliable sync method.  If the primary key has been defined as one of the fields in the row object, then it's value will be used to find and existing record.  If there is no existing record, a new one will be inserted with the defined field values.  If an existing row is found, this record will be compared against the fields defined in the row object and any differences will be updated on the existing record.

!!! notice
    If a database column is not defined in the data sync row object, it's value will not be changed.  If you want to set ensure such columns are empty, simply define them in the row object with a `null` value.

!!! warning
    The caveat with using primary key mode, is that you need to define primary key in a record.  For small systems this should not be a problem, but for large systems with many data sync files, many developers and many records, it can become difficult to keep track of primary keys.  In these situations you can use *Key List Mode* along with *Row Object Field Macros* to link records.

##### Example

In the below example, `id` is the primary key field and so should be defined in each row object.

```json
[
    {
        "table": "test",
        "rows": [
            {
                "id": 1,
                "name": "one",
                "label": "Row Number #1"
            },
            {
                "id": 2,
                "name": "two",
                "label": "Row Number #2"
            }
        ]
    }
]
```

In the above example, the sync engine will look for each record where the `id` column contains the defined value.  If one doesn't exist, a new record will be inserted.  If one does exist, it will ensure that the `name` and `label` columns contain their defined values.  

#### Object Mode

Object mode is the slowest, but most simple mode.  Essentially it will use all the defined column values to find an existing record and only if no record exists will it insert a new record.  

!!! warning 
    Because the existing record lookup is done using all the of the defined data, updates are not possible.

##### Example

```json
[
    {
        "table": "test",
        "rows": [
            {
                "name": "one",
                "label": "Row Number #1"
            },
            {
                "name": "two",
                "label": "Row Number #2"
            }
        ]
    }
]
```

In this example, the sync engine will make sure that the defined rows exist in the database.  If they already exist, then nothing will be changed.

#### Key List Mode

Key list mode is basically a combination of the above two modes, hence why it is sometimes referred to a *hybrid mode*.  In this mode, instead of using the primary key to lookup records, the lookup keys are defined in the *Sync Stage Object*.

##### Example

```json
[
    {
        "table": "test",
        "keys": [ "name" ],
        "rows": [
            {
                "name": "one",
                "label": "Row Number #1"
            },
            {
                "name": "two",
                "label": "Row Number #2"
            }
        ]
    }
]
```

In the above example, the `name` column is used to find an existing record.  If one does not exist an insert is performed using the defined column values.  If a record does exist, it will ensure that the `label` field contains the defined value. 

!!! notice
    It is a good idea to make sure a database index is defined for the columns used in the `keys` attribute.  This will greatly improve performance during data synchronisation.

## Sync Container Array

This array simply contains sync stage objects.  See below for how to define *Sync Stage* objects.

## Sync Stage Objects

*not done yet*

## Row Objects

*not done yet*

## Row Object Value Macros

Row object value macros make it possible to perform simple, optimised queries during the sync process that will lookup and return a value to be stored in the column.  These queries are designed to allow foreign key columns to lookup the reference values that should be stored in this record.

These macros are string field values that are prefixed with `::` and match a very specific pattern.  This means that we wont' interfer with columns values that legitimately contain a `::` prefix.

Macros are defined in the format `::source_table(source_column):field=value,field=value`.  Lookup criteria is comma separated and currently only `AND` criteria is supported.

##### Example

In the below example we have data for two tables. *test_type* and *test* which has a column named *type_id* that references the *test_type* table's *id* column.  In *test_type* the *id* column is a serial primary key.

You can see here that we have a simple macro defined in each field value for the *type_id* column that looks up *id* column of the *internal* record in the *test_type* table.

```json
[
    {
        "table": "test_type",
        "keys": [ "name" ],
        "rows": [
            {
                "name": "internal",
                "label": "Internal Test Type"
            }
        ]
    },
    {
        "table": "test",
        "keys": [ "name" ],
        "rows": [
            {
                "name": "one",
                "label": "Row Number #1",
                "type_id":  "::test_type(id):name=internal"
            },
            {
                "name": "two",
                "label": "Row Number #2",
                "type_id":  "::test_type(id):name=internal"
            }
        ]
    }
]
```

### Macro Optimisation

These queries are optimised to ensure they operate as quickly as possible.  If possible, an SQL query not be performed if the referenced value is defined anywhere in the loaded data sync files.  

This means that in the above example, the data sync engine already knows about a record in the *test_type* table with a name of *internal* and will use it's cached primary key as the reference value.  

If a query IS performed on the database, the results are cached for the life of the data sync process and will used immediately in subsequent lookup macros.  This means that in the above example, if a query was actually performed for the first row, that cached result would be used in the second row because the queries are the same.
