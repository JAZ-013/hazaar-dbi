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
 * "PDO":http://www.php.net/manual/en/book.pdo.php (PHP Data Object) drivers and classes. This
 * approach allows developers to use these classes to access a range of different database servers.
 *
 * PDO has supporting drivers for:
 *
 * * "PostgreSQL":http://www.postgresql.org
 * * "MySQL":http://www.mysql.com
 * * "SQLite":http://www.sqlite.org
 * * "MS SQL Server":http://www.microsoft.com/sqlserver
 * * "Oracle":http://www.oracle.com
 * * "IBM Informix":http://www.ibm.com/software/data/informix
 * * "Interbase":http://www.embarcadero.com/products/interbase
 *
 * Access to database functions is all done using a common class structure.
 *
 * h2. Example Usage
 *
 * <code>
 * $db = new Hazaar\DBI\Adapter();
 * $result = $this->execute('SELECT * FROM users');
 * while($row = $result->fetch()){
 * //Do things with $row here
 * }
 * </code>
 */
class Adapter {

    public static $default_config = array();

    public $config = NULL;

    public $driver;

    // Prepared statements
    private $statements = array();

    private static $connections = array();

    private $schema_manager;

    function __construct($config_env = NULL) {

        if(!$config_env)
            $config_env = APPLICATION_ENV;

        $config = null;

        if ($config_env == NULL || is_string($config_env)) {

            $config = $this->getDefaultConfig($config_env);

        } elseif (is_array($config_env)) {

            $config = new \Hazaar\Map($config_env);

        } elseif ($config_env instanceof \Hazaar\Map) {

            $config = $config_env;

        }

        $this->configure($config);

    }

    public function configure($config){

        if (\Hazaar\Map::is_array($config)) {

            $this->config = clone $config;

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

            $this->connect($dsn, $user, $password);

        }

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

            $config = new \Hazaar\Application\Config('database', $env);

            if(!$config->loaded())
                return null;

            Adapter::$default_config[$env] = $config;

        }

        return Adapter::$default_config[$env];

    }

    public function connect($dsn, $username = NULL, $password = NULL, $driver_options = NULL) {

        $driver = ucfirst(substr($dsn, 0, strpos($dsn, ':')));

        if (!$driver)
            throw new Exception\DriverNotSpecified();

        if (!array_key_exists($driver, Adapter::$connections))
            Adapter::$connections[$driver] = array();

        $hash = md5(serialize(array(
            $driver,
            $dsn,
            $username,
            $password,
            $driver_options
        )));

        if (array_key_exists($hash, Adapter::$connections)) {

            $this->driver = Adapter::$connections[$hash];

        } else {

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
                throw new Exception\ConnectionFailed($dsn);

            Adapter::$connections[$hash] = $this->driver;

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

        }

        return TRUE;

    }

    public function getDriver() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $class = get_class($this->driver);

        return substr($class, strrpos($class, '\\') + 1);

    }

    public function beginTransaction() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->beginTransaction();

    }

    public function commit() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->commit();

    }

    public function getAttribute($option) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->getAttribute($option);

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

    public function inTransaction() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->inTransaction();

    }

    public function lastInsertId() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->lastInsertId();

    }

    public function quote($string) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->quote($string);

    }

    public function rollBack() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->rollback();

    }

    public function setAttribute() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->setAttribute();

    }

    public function errorCode() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->errorCode();

    }

    public function errorInfo() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->errorInfo();

    }

    public function exec($sql) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->exec($sql);

    }

    public function query($sql) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $result = $this->driver->query($sql);

        if($result instanceof \PDOStatement)
            return new Result($result);

        return $result;

    }

    public function exists($table, $criteria = array()) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->table($table)->exists($criteria);

    }

    public function insert($table, $fields, $returning = NULL) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->insert($table, $fields, $returning);

    }

    public function update($table, $fields, $criteria = array()) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->update($table, $fields, $criteria);

    }

    public function delete($table, $criteria) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->delete($table, $criteria);

    }

    public function deleteAll($table) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->deleteAll($table);

    }

    public function __get($tablename) {

        return $this->table($tablename);

    }

    public function __call($tablename, $args) {

        $args = array_merge(array(
            $tablename
        ), $args);

        return call_user_func_array(array(
            $this,
            'table'
        ), $args);

    }

    public function table($name, $alias = NULL) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return new Table($this->driver, $name, $alias);

    }

    public function call($method, $args = array()) {

        $arglist = array();

        foreach($args as $arg)
            $arglist[] = (is_numeric($arg) ? $arg : $this->quote($arg));

        $sql = 'SELECT ' . $method . '(' . implode(',', $arglist) . ');';

        return $this->query($sql);

    }

    /**
     * List all tables currently in the connected database.
     *
     * @since 2.0
     */
    public function listTables() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listTables();

    }

    /**
     * Test that a table exists in the connected database.
     *
     * @param string $table
     *            The name of the table to check for.
     *
     * @param string $schema
     *            The database schema to look in. Defaults to public.
     */
    public function tableExists($table) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->tableExists($table);

    }

    /**
     * Create a new table in the database.
     *
     * @param string $name
     *
     * @param string $columns
     */
    public function createTable($name, $columns) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->createTable($name, $columns);

    }

    public function describeTable($name, $sort = NULL) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->describeTable($name, $sort);

    }

    public function renameTable($from_name, $to_name) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->renameTable($from_name, $to_name);

    }

    public function dropTable($name, $cascade = false) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropTable($name, $cascade);

    }

    public function addColumn($table, $column_spec) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->addColumn($table, $column_spec);

    }

    public function alterColumn($table, $column, $column_spec) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->alterColumn($table, $column, $column_spec);

    }

    public function dropColumn($table, $column) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropColumn($table, $column);

    }

    public function listSequences() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listSequences();

    }

    public function describeSequence($name) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->describeSequence($name);

    }

    public function listIndexes($table = NULL) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listIndexes($table);

    }

    public function createIndex($index_name, $table_name, $idx_info) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->createIndex($index_name, $table_name, $idx_info);

    }

    public function dropIndex($name) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropIndex($name);

    }

    public function listPrimaryKeys($table = NULL){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listConstraints($table, 'PRIMARY KEY');

    }

    public function listForeignKeys($table = NULL){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listConstraints($table, 'FOREIGN KEY');

    }

    public function listConstraints($table = NULL, $type = NULL, $invert_type = FALSE) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listConstraints($table, $type, $invert_type);

    }

    public function addConstraint($name, $info) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->addConstraint($name, $info);

    }

    public function dropConstraint($name, $table, $cascade = false) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropConstraint($name, $table, $cascade);

    }

    /**
     * List views
     */
    public function listViews(){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listViews();

    }

    /**
     * Describe a view
     *
     * @param mixed $name
     * @throws Exception\DriverNotSpecified
     * @return mixed
     */
    public function describeView($name){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->describeView($name);

    }

    /**
     * Create a new view
     * @param mixed $name
     * @throws Exception\DriverNotSpecified
     * @return mixed
     */
    public function createView($name, $sql){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->createView($name, $sql);

    }

    /**
     * Delete/drop a view
     * @param mixed $name
     * @throws Exception\DriverNotSpecified
     * @return mixed
     */
    public function dropView($name, $cascade = false){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropView($name, $cascade);

    }

    /**
     * List functions
     */
    public function listFunctions(){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->listFunctions();

    }

    /**
     * Describe a function
     *
     * @param mixed $name
     * @throws Exception\DriverNotSpecified
     * @return mixed
     */
    public function describeFunction($name){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->describeFunction($name);

    }

    /**
     * Create a new function
     * @param mixed $name
     * @throws Exception\DriverNotSpecified
     * @return mixed
     */
    public function createFunction($name, $spec){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->createFunction($name, $spec);

    }

    /**
     * Delete/drop a function
     * @param mixed $name
     * @throws Exception\DriverNotSpecified
     * @return mixed
     */
    public function dropFunction($name, $cascade = false){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropFunction($name, $cascade);

    }

    public function execCount() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->execCount();

    }

    /**
     * Prepared statements
     */
    public function prepare($sql, $name = null) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $statement = $this->driver->prepare($sql);

        if(!$statement instanceof \PDOStatement)
            throw new \Exception('Driver did not return PDOStatement during prepare!');

        if ($name)
            $this->statements[$name] = $statement;
        else
            $this->statements[] = $statement;

        return $statement;

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

    public function getSchemaManager(){

        if(!$this->schema_manager instanceof SchemaManager)
            $this->schema_manager = new SchemaManager($this);

        return $this->schema_manager;

    }

    /**
     * Returns the current DBI schema versions
     *
     * See: \Hazaar\DBI\SchemaManager::getSchemaVersion()
     *
     * @deprecated
     */
    public function getSchemaVersion() {

        return $this->getSchemaManager()->getSchemaVersion();

    }

    /**
     * Returns a list of all known schema versions
     *
     * See: \Hazaar\DBI\SchemaManager::getSchemaVersions()
     *
     * @deprecated
     */
    public function getSchemaVersions($with_file_obj = false) {

        return $this->getSchemaManager()->getSchemaVersions($with_file_obj);

    }

    /**
     * Returns the version number of the latest schema version
     *
     * See: \Hazaar\DBI\SchemaManager::getLatestSchemaVersion()
     *
     * @deprecated
     */
    public function getLatestSchemaVersion($with_file_obj = false){

        return $this->getSchemaManager()->getLatestSchemaVersion($with_file_obj);

    }

    /**
     * Checks if the current DBI schema is the latest versions
     *
     * See: \Hazaar\DBI\SchemaManager::isSchemaLatest()
     *
     * @deprecated
     */
    public function isSchemaLatest(){

        return $this->getSchemaManager()->isSchemaLatest();

    }

    /**
     * Snapshot the database schema and create a new schema version with migration replay files.
     *
     * See: \Hazaar\DBI\SchemaManager::snapshot()
     *
     * @deprecated
     */
    public function snapshot($comment = null, $test = false){

        return $this->getSchemaManager()->snapshot($comment, $test);

    }

    /**
     * Database migration method.
     *
     * See: \Hazaar\DBI\SchemaManager::migrate()
     *
     * @deprecated
     */
    public function migrate($version = null, $force_data_sync = false, $test = false){

        return $this->getSchemaManager()->migrate($version, $force_data_sync, $test);

    }

    /**
     * Takes a schema definition and creates it in the database.
     *
     * See: \Hazaar\DBI\SchemaManager::createSchema()
     *
     * @deprecated
     */
    public function createSchema($schema){

        return $this->getSchemaManager()->createSchema($schema);

    }

    /**
     * Synchonise schema data with the database
     *
     * See: \Hazaar\DBI\SchemaManager::syncSchemaData()
     *
     * @deprecated
     */
    public function syncSchemaData($data_schema = null, $test = false){

        return $this->getSchemaManager()->syncSchemaData();

    }

}
