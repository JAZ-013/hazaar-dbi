# Transparent Table/Column Encryption

## What is it?

Hazaar DBI has the ability to encrypt data automatically on it's way through to the database.  Once enabled, this encryption is performed in the DBI layer without ANY changes to code for insert, update or select.  This not only adds an extra layer of security _over the wire_, but allows selected data to be stored securely in a database so that only the application can decrypt it.

Encryption can be activated on a whole table, or on individually selected columns in a table.  In each case, only string values are supported (why would you want to encrypt and number or boolean???), and due to the encryption producing varying length data, these columns should be created as `TEXT` type columns to ensure there is enough column storage to hold the encrypted values.

## Enabling Encryption

Encryption requires a unique encryption key that is used to encrypt and decrypt data.  This key can be pretty much anything you want but a decent-sized string of randomly generated characters is recommended.  

!!! note
    Only _Linux_ you can use the `pwgen` program to generate your key.  Execute `pwgen 16` to get some random strings of 16 characters long to choose from.

Once you have your key ready to go you can update your database configuration to enable encryption.  Let's quickly see what that looks like first.

```json
{
    "development": {
        ... _ other config options ...
        "encrypt": {
            "key": "{your encryption key}" <--Optional and only recommended for testing
            "table": {
                "users": [ "password" ],
                "credit_data": true
            }
        }
    }
}
```

In the above example we enable encryption on two tables.  In the _users_ table, a single column named _password_ will be encrypted.  In the *credit_data* table, all string columns will be encrypted.

!!! Danger
    You can see that the _key_ option was used to specify the encryption key.  This feature has been added to ease use of encryption during development but is not recommended for production systems.

## Keeping Your Key Safe

While specifying the key in the configuration is a quick and easy way to get up and running with encryption, it certainly is not recommended for production systems.  For these systems, security is priority so we have the ability to store our key in the config directory in a file name *.db_key* (by default).

So using your favourite text editor, simply open the key file and pop in your key.  On _Linux_ you can do something like this:

```
> echo "{your encryption key}" > {APPLICATION_PATH}/application/configs/.db_key
```

Where _your encryption key_ is (obviously) YOUR ENCRYPTION KEY and *APPLICATION_PATH* is your root application path that contains the _application_ and _public_ directories.

## Configuration Options

Below is the full list of available configuration options in the `encrypt` config object of the database config.

### `cipher` (default: aes-256-ctr)

The encryption algorithm to use to encrypt/decrypt data.  This can be ANY algorithm supported by the OpenSSL in PHP.  You can use the `openssl_get_cipher_methods()` function to get a list of available algorithms on your system.  See the PHP documentation for [openssl-get-cipher-methods](https://www.php.net/manual/en/function.openssl-get-cipher-methods.php) for more information.

### `table`

The `table` object is a key/value object where the _key_ is the name of the table to encrypt and the _value_ is either a boolean to enable/disable whole table encryption, or is an `Array` of column names that should be encrypted.

#### Encrypt a whole table

```json
{
    "tablename": true
}
```

#### Encrypt selected columns in a table

```json
{
    "tablename": [ "column1", "column2", "column3" ]
}
```

### `checkstring` (default: !!)

The _check string_ is used to detect if the decryption has been successful.  It is just 1 or 2 character that are prefixed to value before encryption, and checked that they exist after decryption to indicate success before being stripped off again.  This can be anything but 2 characters is recommended to keep it short but allow good detection reliability in case a single character _magically_ decrypts to the correct character.  Normally the default is fine.

### `key`

It's not recommended that this is used in a production environment.  Specify your encryption key here to get up and running quickly.

### `keyfile` (default: .db_key)

The `keyfile` option can be used to specify the path to an alternative key file.  This can be either a relative path to the _configs_ directory, or an absolute path which allows you to keep the file in an even more secure location outside of the application path.

!!! trick
    You can use the `keyfile` option to point to the _.key_ file that is used for encrypting files to reduce the number of key files you need to manage.
    