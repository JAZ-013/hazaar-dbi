<?php

/**
 * @file        Hazaar/DBI/DBI.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2012 Jamie Carl (http://www.hazaarlabs.com)
 */
namespace Hazaar\DBI;

/**
 * @brief Relational Database Interface
 *
 * @detail The DB Adapter module provides classes for access to relational database via
 * [PDO](http://www.php.net/manual/en/book.pdo.php) (PHP Data Object) drivers and classes. This
 * approach allows developers to use these classes to access a range of different database servers.
 *
 * PDO has supporting drivers for:
 *
 * * [PostgreSQL](http://www.postgresql.org)
 * * [MySQL](http://www.mysql.com)
 * * [SQLite](http://www.sqlite.org)
 * * [MS SQL Server](http://www.microsoft.com/sqlserver)
 * * [Oracle](http://www.oracle.com)
 * * [IBM Informix](http://www.ibm.com/software/data/informix)
 * * [Interbase](http://www.embarcadero.com/products/interbase)
 *
 * Access to database functions is all implemented using a common class structure.  This allows developers
 * to create database queries in code without consideration for the underlying SQL.  SQL is generated 
 * "under the hood" using a database specific driver that will automatically take care of any differences
 * in the database servers SQL implementation.
 *
 * ## Example Usage
 *
 * ```php
 * $db = Hazaar\DBI\Adapter::getInstance();
 * 
 * $result = $this->execute('SELECT * FROM users');
 * 
 * while($row = $result->fetch()){
 * 
 * //Do things with $row here
 * 
 * }
 * ```
 */
class Adapter {

    public static $default_config = array();

    public $config = NULL;

    private $options;

    public $driver;

    // Prepared statements
    private $statements = array();

    private static $connections = array();

    private $schema_manager;

    static public $default_checkstring = '!!';

    static private $instances = array();

    function __construct($config_env = NULL) {

        if(!$config_env)
            $config_env = APPLICATION_ENV;

        $config = null;

        if ($config_env === NULL || is_string($config_env))
            $config = $this->getDefaultConfig($config_env);
        elseif (is_array($config_env))
            $config = new \Hazaar\Map($config_env);
        elseif ($config_env instanceof \Hazaar\Map){

            $config = $config_env;

            $config_env = md5(serialize($config));

        }

        if($config !== NULL)
            $this->configure($config);

        if(!array_key_exists($config_env, self::$instances))
            self::$instances[$config_env] = $this;

    }

    /**
     * Return an existing or create a new instance of a DBI Adapter
     * 
     * This function should be the main entrypoint to the DBI Adapter class as it implements instance tracking to reduce
     * the number of individual connections to a database.  Normally, instantiating a new DBI Adapter will create a new
     * connection to the database, even if one already exists.  This may be desired so this functionality still exists.
     * 
     * However, if using `Hazaar\DBI\Adapter::getInstance()` you will be returned an existing instance if one exists allowing
     * a single connection to be used from multiple sections of code, without the need to pass around an existing reference.
     * 
     * NOTE:  Only the first instance created is tracked, so it is still possible to create multiple connections by
     * instantiating the DBI Adapter directly.  The choice is yours.
     */
    static public function getInstance($config_env = null){

        if(array_key_exists($config_env, self::$instances))
            return self::$instances[$config_env];

        return new Adapter($config_env);

    }

    public function configure($config){

        if (!\Hazaar\Map::is_array($config))
            return false;

        $config = clone $config;

        $this->options = array_filter($config->toArray(), function($key) use($config){
            if($key === 'encrypt'){
                unset($config[$key]);
                return true;
            }
            return  false;
        }, ARRAY_FILTER_USE_KEY);

        $this->config = $config;

        return $this->reconfigure();

    }

    private function reconfigure($reconnect = false){

        $user = ( $this->config->has('user') ? $this->config->user : null );

        $password = ( $this->config->has('password') ? $this->config->password : null );

        if ($this->config->has('dsn')){

            $dsn = $this->config->dsn;

        }else{

            $DBD = Adapter::getDriverClass($this->config->driver);

            if(!class_exists($DBD))
                return false;

            $dsn = $DBD::mkdsn($this->config);

        }

        $driver = ucfirst(substr($dsn, 0, strpos($dsn, ':')));

        if (!array_key_exists($driver, Adapter::$connections))
            Adapter::$connections[$driver] = array();

        $hash = md5(serialize($this->config->toArray()));

        if ($reconnect !== true && array_key_exists($hash, Adapter::$connections)) {

            $this->driver = Adapter::$connections[$hash];

        } else {

            if(!$this->connect($dsn, $user, $password, null, $reconnect))
                throw new Exception\ConnectionFailed($this->config['host']);

            Adapter::$connections[$hash] = $this->driver;

            if($this->config->has('schema'))
                $this->driver->setSchemaName($this->config->get('schema'));

        }

        if(array_key_exists('encrypt', $this->options) && !array_key_exists('key', $this->options['encrypt'])){

            $keyfile = \Hazaar\Loader::getFilePath(FILE_PATH_CONFIG, ake($this->options['encrypt'], 'keyfile', '.db_key'));

            if(!file_exists($keyfile))
                throw new \Hazaar\Exception('DBI keyfile is missing.  Database encryption will not work!');

            $this->options['encrypt']['key'] = trim(file_get_contents($keyfile));

        }

        return true;

    }

    private function checkConfig(){

        if(!$this->config)
            throw new Exception\NotConfigured();

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return true;

    }

    static public function getDriverClass($driver){

        return 'Hazaar\DBI\DBD\\' . ucfirst($driver);

    }

    static public function setDefaultConfig($config, $env = NULL){

        if(!$env)
            $env = APPLICATION_ENV;

        Adapter::$default_config[$env] = $config;

    }

    static public function getDefaultConfig($env = NULL) {

        if(!$env)
            $env = APPLICATION_ENV;

        if (!array_key_exists($env, Adapter::$default_config)){

            $defaults = array(
                'timezone' => date_default_timezone_get()
            );

            $config = new \Hazaar\Application\Config('database', $env, $defaults, FILE_PATH_CONFIG, true);

            if(!$config->loaded())
                return null;

            Adapter::$default_config[$env] = $config;

        }

        return Adapter::$default_config[$env];

    }

    public function connect($dsn, $username = NULL, $password = NULL, $driver_options = NULL, $reconnect = false) {

        $driver = ucfirst(substr($dsn, 0, strpos($dsn, ':')));

        if (!$driver)
            throw new Exception\DriverNotSpecified();

        $DBD = Adapter::getDriverClass($driver);

        if (!class_exists($DBD))
            throw new Exception\DriverNotFound($driver);

        $this->driver = new $DBD(array_unflatten(substr($dsn, strpos($dsn, ':') + 1)));

        if (!$driver_options)
            $driver_options = array();

        $driver_options = array_replace(array(
            \PDO::ATTR_STRINGIFY_FETCHES => FALSE,
            \PDO::ATTR_EMULATE_PREPARES => FALSE
        ), $driver_options);

        if (!$this->driver->connect($dsn, $username, $password, $driver_options))
            return false;

        if ($this->config->has('master')){

            $master_config = clone $this->config;

            $master_config->extend($this->config->master);

            unset($master_config->master);

            $DBD2 = Adapter::getDriverClass($master_config->driver);

            $master = new $DBD2($master_config);

            if(!$master->connect($DBD2::mkdsn($master_config), $username, $password, $driver_options))
                throw new Exception\ConnectionFailed($dsn);

            $this->driver->setMasterDBD($master);

        }

        if($this->config->has('timezone'))
            $this->setTimezone($this->config['timezone']);


        return TRUE;

    }

    public function getDriver() {

        $this->checkConfig();

        $class = get_class($this->driver);

        return substr($class, strrpos($class, '\\') + 1);

    }

    public function getAvailableDrivers() {

        $drivers = array();

        $dir = new \Hazaar\File\Dir(dirname(__FILE__) . '/DBD');

        while($file = $dir->read()) {

            if (preg_match('/class (\w*) extends BaseDriver\W/m', $file->getContents(), $matches))
                $drivers[] = $matches[1];

        }

        return $drivers;

    }

    public function query($sql) {

        $result = false;

        $retries = 0;

        $this->checkConfig();

        while($retries++ < 3){

            $result = $this->driver->query($sql);

            if($result instanceof \PDOStatement)
                return new Result($this, $result, $this->options);

            $error = $this->errorCode();

            if(!($error === '57P01' || $error === 'HY000'))
                break;

            $this->reconfigure(true);

        }

        return $result;

    }

    public function exists($table, $criteria = array()) {

        $this->checkConfig();

        return $this->table($table)->exists($criteria);

    }

    public function __get($tablename) {

        return $this->table($tablename);

    }

    public function __call($arg, $args) {

        $this->checkConfig();

        if(method_exists($this->driver, $arg))
            return call_user_func_array(array($this->driver, $arg), $args);

        return $this->table($arg, ake($args, 0));

    }

    public function table($name, $alias = NULL) {

        $this->checkConfig();

        return new Table($this, $name, $alias, $this->options);

    }

    public function from($name, $alias = null){

        return $this->table($name, $alias);
        
    }

    public function call($method, $args = array()) {

        $arglist = array();

        foreach($args as $arg)
            $arglist[] = (is_numeric($arg) ? $arg : $this->quote($arg));

        $sql = 'SELECT ' . $method . '(' . implode(',', $arglist) . ');';

        return $this->query($sql);

    }

    public function listPrimaryKeys($table = NULL){

        $this->checkConfig();

        return $this->driver->listConstraints($table, 'PRIMARY KEY');

    }

    public function listForeignKeys($table = NULL){

        $this->checkConfig();

        return $this->driver->listConstraints($table, 'FOREIGN KEY');

    }

    public function listConstraints($table = NULL, $type = NULL, $invert_type = FALSE) {

        $this->checkConfig();

        return $this->driver->listConstraints($table, $type, $invert_type);

    }

    /**
     * Prepared statements
     */
    public function prepare($sql, $name = null) {

        $this->checkConfig();

        $statement = $this->driver->prepare($sql);

        if(!$statement instanceof \PDOStatement)
            throw new \Hazaar\Exception('Driver did not return PDOStatement during prepare!');

        if ($name)
            $this->statements[$name] = $statement;
        else
            $this->statements[] = $statement;

        return new Result($this, $statement, $this->options);

    }

    public function execute($name, $input_parameters) {

        if (!($statement = ake($this->statements, $name)) instanceof \PDOStatement)
            return false;

        if (!is_array($input_parameters))
            $input_parameters = array($input_parameters);

        return $statement->execute($input_parameters);

    }

    public function getPreparedStatements() {

        return $this->statements;

    }

    public function listPreparedStatements() {

        return array_keys($this->statements);

    }

    /**
     * Returns an instance of the Hazaar\DBI\Schema\Manager for managing database schema versions.
     *
     * @return Schema\Manager
     */
    public function getSchemaManager(){

        if(!$this->schema_manager instanceof Schema\Manager)
            $this->schema_manager = new Schema\Manager($this);

        return $this->schema_manager;

    }

    /**
     * Perform and "upsert"
     * 
     * An upsert is an INSERT, that when it fails, columns can be updated in the existing row.
     * 
     * @param string $table The table to insert a record into.
     * @param array $fields The fields to be inserted.
     * @param string $returning A column to return when the row is inserted (usually the primary key).
     * @param array $update_columns The names of the columns to be updated if the row exists.
     * @param array $update_where Not used yet
     */
    public function insert($table, $fields, $returning = null, $update_columns = null, $update_where = null){

        $result = $this->driver->insert($table, $this->encrypt($table, $fields), $returning, $update_columns, $update_where);

        if($result instanceof \PDOStatement){

            $result = new Result($this, $result, $this->options);

            return $result->fetch();

        }
        
        return $result;

    }

    public function update($table, $fields, $criteria = array(), $from = array(), $returning = array()){

        $result = $this->driver->update($table, $this->encrypt($table, $fields), $criteria, $from, $returning);

        if($result instanceof \PDOStatement){

            $result = new Result($this, $result, $this->options);

            return ((is_string($returning) && $returning) || (is_array($returning) && count($returning) > 0)) ? $result->fetchAll() : $result->fetch();

        }

        return $result;

    }

    public function encrypt($table, &$data){

        if(is_array($table) && isset($table[0]))
            $table = $table[0];

        if($data === null
            || !(is_array($data) && count($data) > 0)
            || ($encrypt = ake($this->options, 'encrypt', false)) === false
            || ($encrypted_fields = ake(ake($encrypt, 'table'), $table)) === null)
            return $data;

        $cipher = ake($encrypt, 'cipher', 'aes-256-ctr');

        $key = ake($encrypt, 'key', '0000');

        $checkstring = ake($encrypt, 'checkstring', Adapter::$default_checkstring);

        foreach($data as $key => &$value){

            if(!in_array($key, $encrypted_fields))
                continue;

            if(!is_string($value))
                throw new \Hazaar\Exception('Trying to encrypt non-string field: ' . $key);

            $iv = openssl_random_pseudo_bytes(openssl_cipher_iv_length($cipher));

            $value = base64_encode($iv . openssl_encrypt($checkstring . $value, $cipher, $key, OPENSSL_RAW_DATA, $iv));

        }

        return $data;

    }

    /**
     * TRUNCATE empty a table or set of tables
     *
     * TRUNCATE quickly removes all rows from a set of tables. It has the same effect as an unqualified DELETE on
     * each table, but since it does not actually scan the tables it is faster. Furthermore, it reclaims disk space
     * immediately, rather than requiring a subsequent VACUUM operation. This is most useful on large tables.
     *
     * @param mixed $table_name         The name of the table(s) to truncate.  Multiple tables are supported.
     * @param mixed $only               Only the named table is truncated. If FALSE, the table and all its descendant tables (if any) are truncated.
     * @param mixed $restart_identity   Automatically restart sequences owned by columns of the truncated table(s).  The default is to no restart.
     * @param mixed $cascade            If TRUE, automatically truncate all tables that have foreign-key references to any of the named tables, or
     *                                  to any tables added to the group due to CASCADE.  If FALSE, Refuse to truncate if any of the tables have
     *                                  foreign-key references from tables that are not listed in the command. FALSE is the default.
     * @return boolean
     */
    public function truncate($table_name, $only = false, $restart_identity = false, $cascade = false){

        return $this->driver->truncate($table_name, $only, $restart_identity, $cascade);

    }

    public function errorException($msg = null){

        if($err = $this->errorinfo()){

            if($err[1] !== null)
                $msg .= (($msg !== null) ? ' SQL ERROR ' . $err[0] . ': ' : $err[0] . ': ') . $err[2];

            if($msg)
                return new \Exception($msg, $err[1]);

        }

        return new \Exception('Unknown DBI Error!');

    }

   
    /**
     * Parse an SQL query into a DBI Table query so that it can be manipulated
     * 
     * @param string $sql The SQL query to parse
     * 
     * @return \Hazaar\DBI\Table
     */
    public function parseSQL($sql){

        return new Table\SQL($this, $sql);

    }

    /**
     * Return the name of the currently set default schema
     */
    public function getSchema(){

        return $this->driver->getSchemaName();

    }

}
