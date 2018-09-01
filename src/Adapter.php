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

    private static $default_config = array();

    private static $connections = array();

    private $config = NULL;

    private $driver;

    private $schema_file;

    private $data_file;

    private $migration_log = array();

    // Prepared statements
    private $statements = array();

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

        $this->schema_file = realpath(APPLICATION_PATH . DIRECTORY_SEPARATOR . '..') . DIRECTORY_SEPARATOR . 'db' . DIRECTORY_SEPARATOR . 'schema.json';

        $this->data_file = realpath(APPLICATION_PATH . DIRECTORY_SEPARATOR . '..') . DIRECTORY_SEPARATOR . 'db' . DIRECTORY_SEPARATOR . 'data.json';

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
    public function dropView($name){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->dropView($name);

    }

    public function execCount() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        return $this->driver->execCount();

    }

    public function getSchemaVersion() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        if (!$this->schema_info->exists())
            return false;

        $result = $this->schema_info->find(array(), array(
            'version'
        ))->sort('version', true);

        if ($row = $result->row())
            return $row['version'];

        return false;

    }

    public function getSchemaVersions($with_file_obj = false) {

        $db_dir = dirname($this->schema_file);

        $migrate_dir = $db_dir . '/migrate';

        $versions = array();

        /**
         * Get a list of all the available versions
         */
        $dir = new \Hazaar\File\Dir($migrate_dir);

        if ($dir->exists()) {

            while($file = $dir->read()) {

                if (preg_match('/(\d*)_(\w*)/', $file, $matches)) {

                    $version = $matches[1];

                    if ($with_file_obj)
                        $versions[$version] = $file;

                    else
                        $versions[$version] = str_replace('_', ' ', $matches[2]);

                }

            }

            ksort($versions);

        }

        return $versions;

    }

    public function getLatestSchemaVersion($with_file_obj = false){

        $versions = $this->getSchemaVersions($with_file_obj);

        end($versions);

        return key($versions);

    }

    public function isSchemaLatest(){

        return $this->getLatestSchemaVersion() == $this->getSchemaVersion();

    }

    /**
     * Creates the info table that stores the version info of the current database.
     */
    private function createInfoTable() {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $table = 'schema_info';

        if (!$this->tableExists($table)) {

            $this->createTable($table, array(
                'version' => array(
                    'data_type' => 'int8',
                    'not_null' => true,
                    'primarykey' => true
                )
            ));

            return true;
        }

        return false;

    }

    private function getColumn($needle, $haystack, $key = 'name') {

        foreach($haystack as $item) {

            if (array_key_exists($key, $item) && $item[$key] == $needle)
                return $item;
        }

        return null;

    }

    private function colExists($needle, $haystack, $key = 'name') {

        return ($this->getColumn($needle, $haystack, $key) !== null) ? true : false;

    }

    private function getColumnDiff($new, $old) {

        $this->log("Column diff is not implemented yet!");

        return null;

    }

    private function getTableDiffs($new, $old) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $diff = array();

        /**
         * Look for any differences between the existing schema file and the current schema
         */
        $this->log("Looking for new and updated columns");

        foreach($new as $col) {

            /*
             * Check if the column is in the schema and if so, check it for changes
             */
            if (!$this->colExists($col['name'], $old)) {

                $this->log("+ Column '$col[name]' is new.");

                $diff['add'][$col['name']] = $col;
            }
        }

        $this->log("Looking for removed columns");

        foreach($old as $col) {

            if (!$this->colExists($col['name'], $new)) {

                $this->log("- Column '$col[name]' has been removed.");

                $diff['drop'][] = $col['name'];
            }
        }

        return $diff;

    }

    /**
     * Snapshot the database schema and create a new schema version with migration replay files.
     *
     * This method is used to create the database schema migration files. These files are used by the \Hazaar\Adapter::migrate()
     * method to bring a database up to a certain version. Using this method simplifies creating these migration files
     * and removes the need to create them manually when there are trivial changes.
     *
     * When developing your project
     *
     * Currently only the following changes are supported:
     * * Table creation, removal and rename.
     * * Column creation, removal and alteration.
     * * Index creation and removal.
     *
     * p(notice notice-info). Table rename detection works by comparing new tables with removed tables for tables that have the same columns. Because
     * of this, rename detection will not work if columns are added or removed at the same time the table is renamed. If you want to
     * rename a table, make sure that this is the only operation being performed on the table for a single snapshot. Modifying other
     * tables will not affect this. If you want to rename a table AND change it's column layout, make sure you do either the rename
     * or the modifications first, then snapshot, then do the other operation before snapshotting again.
     *
     * @param string $comment
     *            A comment to add to the migration file.
     *
     * @throws \Exception
     *
     * @return boolean True if the snapshot was successful. False if no changes were detected and nothing needed to be done.
     */
    public function snapshot($comment = null, $test = false) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $this->log('Snapshot process starting');

        if ($test)
            $this->log('Test mode ENABLED');

        if ($versions = $this->getSchemaVersions()) {

            end($versions);

            $latest_version = key($versions);

        } else {

            $latest_version = 0;

        }

        $version = $this->getSchemaVersion();

        if ($latest_version > $version)
            throw new \Exception('Snapshoting a database that is not at the latest schema version is not supported.');

        $this->beginTransaction();

        $db_dir = dirname($this->schema_file);

        if (!is_dir($db_dir)) {

            if (file_exists($db_dir))
                throw new \Exception('Unable to create database migration directory.  It exists but is not a directory!');

            mkdir($db_dir);

        }

        try {

            $result = $this->query('SELECT CURRENT_TIMESTAMP');

            if (!$result instanceof Result)
                throw new \Exception('No rows returned!');

            $this->log("Starting at: " . $result->fetchColumn(0));

        }
        catch(\Exception $e) {

            $this->log('There was a problem connecting to the database!');

            $this->log($e->getMessage());

            return false;

        }

        $init = false;

        /**
         * Load the existing stored schema to use for comparison
         */
        $schema = (file_exists($this->schema_file) ? json_decode(file_get_contents($this->schema_file), true) : array());

        if ($schema) {

            $this->log('Existing schema loaded.');

            $this->log(count(ake($schema, 'tables', array())) . ' tables defined.');

            $this->log(count(ake($schema, 'indexes', array())) . ' indexes defined.');

        } else {

            if (!$comment)
                $comment = 'Initial Snapshot';

            $this->log('No existing schema.  Creating initial snapshot.');

            $init = true;

        }

        if (!$comment)
            $comment = "New Snapshot";

        $this->log('Comment: ' . $comment);

        /**
         * Prepare a new version number based on the current date and time
         */
        $version = date('YmdHis');

        /**
         * Stores the schema as it currently exists in the database
         */
        $current_schema = array('version' => $version);

        /**
         * Stores only changes between $schema and $current_schema
         */
        $changes = array();

        if($init)
            $changes['down']['raise'] = 'Can not revert initial snapshot';

        /**
         * Check for any new tables or changes to existing tables.
         * This pretty much looks just for tables to add and
         * any columns to alter.
         */
        foreach($this->listTables() as $table) {

            $name = $table['name'];

            if ($name == 'schema_info')
                continue;

            $this->log("Processing table '$name'.");

            if(!($cols = $this->describeTable($name, 'ordinal_position')))
                throw new \Exception("Error getting table definition for table '$name'.  Does the connected user have the correct permissions?");

            $current_schema['tables'][$name] = $cols;

            //BEGIN PROCESSING TABLE
            if (array_key_exists('tables', $schema) && array_key_exists($name, $schema['tables'])) {

                $this->log("Table '$name' already exists.  Checking differences.");

                $diff = $this->getTableDiffs($cols, $schema['tables'][$name]);

                if (count($diff) > 0) {

                    $this->log("> Table '$name' has changed.");

                    $changes['up']['alter']['table'][$name] = $diff;

                    foreach($diff as $diff_mode => $col_diff) {

                        $diff_mode = ($diff_mode == 'add') ? 'drop' : 'add';

                        foreach($col_diff as $col_name => $col_info) {

                            if ($diff_mode == 'add') {

                                $info = $this->getColumn($col_info, $schema['tables'][$name]);

                                $changes['down']['alter']['table'][$name][$diff_mode][$col_name] = $info;

                            } else {

                                $changes['down']['alter']['table'][$name][$diff_mode][] = $col_name;

                            }

                        }

                    }

                } else {

                    $this->log("No changes to '$name'.");

                }

            } else { // Table doesn't exist, so we add a command to create the whole thing

                $this->log("+ Table '$name' has been created.");

                $changes['up']['create']['table'][] = array(
                    'name' => $name,
                    'cols' => $cols
                );

                if (!$init)
                    $changes['down']['remove']['table'][] = $name;

            } //END PROCESSING TABLES

            //BEGIN PROCESSING CONSTRAINTS
            $constraints = $this->listConstraints($name);

            if(count($constraints) > 0){

                if(!array_key_exists('constraints', $current_schema))
                    $current_schema['constraints'] = array();

                $current_schema['constraints'][$name] =  $constraints;

            }

            if (array_key_exists('constraints', $schema) && array_key_exists($name, $schema['constraints'])) {

                $this->log("Looking for new constraints on table '$name'.");

                //Look for new constraints
                foreach($constraints as $constraint_name => $constraint){

                    if(!array_key_exists($constraint_name, $schema['constraints'][$name])){

                        $this->log("+ Added new constraint '$constraint_name' on table '$name'.");

                        $changes['up']['create']['constraint'][] = array_merge($constraint, array(
                            'name' => $constraint_name,
                        ));

                        //If the constraint was added at the same time as the table, we don't need to add the removes
                        if(! $init && !(array_key_exists('down', $changes)
                            && array_key_exists('remove', $changes['down'])
                            && array_key_exists('table', $changes['down']['remove'])
                            && in_array($name, $changes['down']['remove']['table'])))

                            $changes['down']['remove']['constraint'][] = array('table' => $name, 'name' => $constraint_name);

                    }

                }

                $this->log('Looking for removed constraints');

                //Look for any removed constraints.  If there are no constraints in the current schema, then all have been removed.
                if(array_key_exists('constraints', $current_schema) && array_key_exists($name, $current_schema['constraints']))
                    $missing = array_diff(array_keys($schema['constraints'][$name]), array_keys($current_schema['constraints'][$name]));
                else
                    $missing = array_keys($schema['constraints'][$name]);

                if (count($missing) > 0) {

                    foreach($missing as $constraint) {

                        $this->log("- Constraint '$constraint' has been removed from table '$name'.");

                        $idef = $schema['constraints'][$name][$constraint];

                        $changes['up']['remove']['constraint'][] = array('table' => $name, 'name' => $constraint);

                        $changes['down']['create']['constraint'][] = array_merge($idef, array(
                            'name' => $constraint,
                            'table' => $name
                        ));

                    }

                }

            }elseif(count($constraints) > 0){

                foreach($constraints as $constraint_name => $constraint){

                    $this->log("+ Added new constraint '$constraint_name' on table '$name'.");

                    $changes['up']['create']['constraint'][] = array_merge($constraint, array(
                        'name' => $constraint_name,
                    ));

                    if (!$init)
                        $changes['down']['remove']['constraint'][] = array('table' => $name, 'name' => $constraint_name);

                }

            } //END PROCESSING CONSTRAINTS

            //BEGIN PROCESSING INDEXES
            $indexes = $this->listIndexes($name);

            if(count($indexes) > 0){

                foreach($indexes as $index_name => $index){

                    //Check if the index is actually a constraint
                    if(array_key_exists('constraints', $current_schema)
                        && array_key_exists($name, $current_schema['constraints'])
                        && array_key_exists($index_name, $current_schema['constraints'][$name]))
                        continue;

                    if(!array_key_exists('indexes', $current_schema))
                        $current_schema['indexes'] = array();

                    $current_schema['indexes'][$name][$index_name] =  $index;

                }

            }

            if (array_key_exists('indexes', $schema) && array_key_exists($name, $schema['indexes'])) {

                $this->log("Looking for new indexes on table '$name'.");

                //Look for new indexes
                foreach($indexes as $index_name => $index){

                    //Check if the index is actually a constraint
                    if(array_key_exists('constraints', $current_schema)
                        && array_key_exists($name, $current_schema['constraints'])
                        && array_key_exists($index_name, $current_schema['constraints'][$name]))
                        continue;

                    if(array_key_exists($index_name, $schema['indexes'][$name]))
                        continue;

                    $this->log("+ Added new index '$index_name' on table '$name'.");

                    $changes['up']['create']['index'][] = $index;

                    if(!$init)
                        $changes['down']['remove']['index'][] = $index_name;

                }

                $this->log('Looking for removed indexes');

                //Look for any removed indexes.  If there are no indexes in the current schema, then all have been removed.
                if(array_key_exists('indexes', $current_schema) && array_key_exists($name, $current_schema['indexes']))
                    $missing = array_diff(array_keys($schema['indexes'][$name]), array_keys($current_schema['indexes'][$name]));
                else
                    $missing = array_keys($schema['indexes'][$name]);

                if (count($missing) > 0) {

                    foreach($missing as $index) {

                        $this->log("- Index '$index' has been removed from table '$name'.");

                        $idef = $schema['indexes'][$name][$index];

                        $changes['up']['remove']['index'][] = $index;

                        $changes['down']['create']['index'][] = array_merge($idef, array(
                            'name' => $index,
                            'table' => $name,
                        ));

                    }

                }

            }elseif(count($indexes) > 0){

                foreach($indexes as $index_name => $index){

                    //Check if the index is actually a constraint
                    if(array_key_exists('constraints', $current_schema)
                        && array_key_exists($name, $current_schema['constraints'])
                        && array_key_exists($index_name, $current_schema['constraints'][$name]))
                        continue;

                    $this->log("+ Added new index '$index_name' on table '$name'.");

                    $changes['up']['create']['index'][] = array_merge($index, array(
                        'name' => $index_name,
                        'table' => $name,
                    ));

                    if (!$init)
                        $changes['down']['remove']['index'][] = $index_name;

                }

            } //END PROCESSING INDEXES

        }

        if (array_key_exists('tables', $schema)) {

            /**
             * Now look for any tables that have been removed
             */
            $missing = array_diff(array_keys($schema['tables']), array_keys($current_schema['tables']));

            if (count($missing) > 0) {

                foreach($missing as $table) {

                    $this->log("- Table '$table' has been removed.");

                    $changes['up']['remove']['table'][] = $table;

                    $changes['down']['create']['table'][] = array(
                        'name' => $table,
                        'cols' => $schema['tables'][$table]
                    );

                    //Add any constraints that were on this table to the down script so they get re-created
                    if(array_key_exists('constraints', $schema)
                        && array_key_exists($table, $schema['constraints'])){

                        $changes['down']['create']['constraint'] = array();

                        foreach($schema['constraints'][$table] as $constraint_name => $constraint){

                            $changes['down']['create']['constraint'][] = array_merge($constraint, array(
                                'name' => $constraint_name,
                                'table' => $table
                            ));

                        }

                    }

                    //Add any indexes that were on this table to the down script so they get re-created
                    if(array_key_exists('indexes', $schema)
                        && array_key_exists($table, $schema['indexes'])){

                        $changes['down']['create']['index'] = array();

                        foreach($schema['indexes'][$table] as $index_name => $index){

                            $changes['down']['create']['index'][] = array_merge($index, array(
                                'name' => $index_name,
                                'table' => $table
                            ));

                        }

                    }

                }

            }

        }

        /**
         * Now compare the create and remove changes to see if a table is actually being renamed
         */
        if (isset($changes['up']['create']) && isset($changes['up']['remove']['table'])) {

            $this->log('Looking for renamed tables.');

            if(array_key_exists('table', $changes['up']['create'])){

                foreach($changes['up']['create']['table'] as $create) {

                    foreach($changes['up']['remove']['table'] as $remove_key => $remove) {

                        if(!array_udiff($schema['tables'][$remove], $create['cols'], function($a, $b){
                            if($a['name'] == $b['name']) return 0;
                            return (($a['name'] > $b['name']) ? 1: -1);
                        })){

                            $this->log("> Table '$remove' has been renamed to '{$create['name']}'.", LOG_NOTICE);

                            $changes['up']['rename']['table'][] = array(
                                'from' => $remove,
                                'to' => $create['name']
                            );

                            $changes['down']['rename']['table'][] = array(
                                'from' => $create['name'],
                                'to' => $remove
                            );

                            /**
                             * Clean up the changes
                             */
                            unset($changes['up']['create'][$create_key]);

                            if (count($changes['up']['create']) == 0)
                                unset($changes['up']['create']);

                            unset($changes['up']['remove']['table'][$remove_key]);

                            if (count($changes['up']['remove']['table']) == 0)
                                unset($changes['up']['remove']['table']);

                            if (count($changes['up']['remove']) == 0)
                                unset($changes['up']['remove']);

                            foreach($changes['down']['remove']['table'] as $down_remove_key => $down_remove) {

                                if ($down_remove == $create['name'])
                                    unset($changes['down']['remove']['table'][$down_remove_key]);
                            }

                            foreach($changes['down']['create'] as $down_create_key => $down_create) {

                                if ($down_create['name'] == $remove)
                                    unset($changes['down']['create'][$down_create_key]);

                            }

                            if (count($changes['down']['create']) == 0)
                                unset($changes['down']['create']);

                            if (count($changes['down']['remove']['table']) == 0)
                                unset($changes['down']['remove']['table']);

                            if (count($changes['down']['remove']) == 0)
                                unset($changes['down']['remove']);
                        }

                    }

                }

            }

        }

        if (count($changes) > 0) {

            if(array_key_exists('up', $changes)){

                if(ake($changes['up'], 'create')){

                    foreach($changes['up']['create'] as $type => $items)
                        $this->log('+ New ' . $type . ' count: ' . count($items));

                }

                if(ake($changes['up'], 'alter')){

                    foreach($changes['up']['alter'] as $type => $items)
                        $this->log('> Changed ' . $type . ' count: ' . count($items));

                }

                if(ake($changes['up'], 'remove')){

                    foreach($changes['up']['remove'] as $type => $items)
                        $this->log('- Removed ' . $type . ' count: ' . count($items));

                }

            }

            if ($test)
                return ake($changes,'up');

            /**
             * Save the migrate diff file
             */
            $migrate_dir = $db_dir . '/migrate';

            if (!file_exists($migrate_dir)) {

                $this->log('Migration directory does not exist.  Creating.');

                mkdir($migrate_dir);

            }

            $migrate_file = $migrate_dir . '/' . $version . '_' . str_replace(' ', '_', trim($comment)) . '.json';

            $this->log("Writing migration file to '$migrate_file'");

            file_put_contents($migrate_file, json_encode($changes, JSON_PRETTY_PRINT));

            /**
             * Merge in static schema elements (like data) and save the current schema file
             */
            if($data = ake($schema, 'data')){

                $this->log("Merging schema data records into current schema");

                $current_schema['data'] = $data;

            }

            $this->log("Saving current schema ($this->schema_file)");

            file_put_contents($this->schema_file, json_encode($current_schema, JSON_PRETTY_PRINT));

            $this->createInfoTable();

            $this->insert('schema_info', array(
                'version' => $version
            ));

            $this->commit();

            return true;

        }

        $this->log('No changes detected.');

        return false;

    }

    /**
     * Database migration method.
     *
     * This method does some fancy database migration magic. It makes use of the 'db' subdirectory in the project directory
     * which should contain the schema.json file. This file is the current database schema definition.
     *
     * A few things can occur here.
     *
     * # If the database schema does not exist, then a new schema will be created using the schema.json schema definition file.
     * This will create the database at the latest version of the schema.
     * # If the database schema already exists, then the current version is checked against the version requested using the
     * $version parameter. If no version is requested ($version is NULL) then the latest version number is used.
     * # If the version numbers are different, then a migration will be performed.
     * # # If the requested version is greater than the current version, the migration mode will be 'up'.
     * # # If the requested version is less than the current version, the migration mode will be 'down'.
     * # All migration files between the two selected versions (current and requested) will be replayed using the migration mode.
     *
     * This process can be used to bring a database schema up to the latest version using database migration files stored in the
     * db/migrate project subdirectory. These migration files are typically created using the \Hazaar\Adapter::snapshot() method
     * although they can be created manually. Take care when using manually created migration files.
     *
     * The migration is performed in a database transaction (if the database supports it) so that if anything goes wrong there
     * is no damage to the database. If something goes wrong, errors will be availabl in the migration log accessible with
     * \Hazaar\Adapter::getMigrationLog(). Errors in the migration files can be fixed and the migration retried.
     *
     * @param string $version The database schema version to migrate to.
     *
     * @return boolean Returns true on successful migration. False if no migration was neccessary. Throws an Exception on error.
     */
    public function migrate($version = null, $force_data_sync = false, $test = false) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $this->log('Migration process starting');

        if ($test)
            $this->log('Test mode ENABLED');

        $mode = 'up';

        $current_version = 0;

        $versions = $this->getSchemaVersions(true);

        $file = new \Hazaar\File($this->schema_file);

        if (!$file->exists())
            throw new \Exception("This application has no schema file.  Database schema is not being managed.");

        if (!($schema = json_decode($file->get_contents(), true)))
            throw new \Exception("Unable to parse the migration file.  Bad JSON?");

        if(!array_key_exists('version', $schema))
            $schema['version'] = 1;

        if(!is_string($version))
            settype($version, 'string');

        if ($version) {

            /**
             * Make sure the requested version exists
             */
            if (!array_key_exists($version, $versions))
                throw new \Exception("Unable to find migration version '$version'.");

        } else {

            if (count($versions) > 0) {
                /**
                 * No version supplied so we grab the last version
                 */
                end($versions);

                $version = key($versions);

                reset($versions);

                $this->log('Migrating database to version: ' . $version);

            } else {

                $version = $schema['version'];

                $this->log('Initialising database at version: ' . $version);

            }

        }

        /**
         * Check that the database exists and can be written to.
         */
        try {

            $this->createInfoTable();

        }
        catch(\PDOException $e) {

            if ($e->getCode() == 7) {

                $name = $this->config->dbname;

                throw new \Exception("Database '$name' does not exist.  Please create the database with owner '{$this->config->user}'.");
            }

            throw new \Exception($e->getMessage());
        }

        /**
         * Get the current version (if any) from the database
         */
        if($this->tableExists('schema_info')){

            if ($result = $this->table('schema_info')->find(array(), array('version'))->sort('version', true)) {

                if($row = $result->fetch())
                    $current_version = $row['version'];

                $this->log("Current database version: " . ($current_version ? $current_version : "None"));

            }

        }

        /**
         * Check to see if we are at the current version first.
         */
        if ($current_version == $version) {

            $this->log("Database is already at version: $version");

            if($force_data_sync)
                $this->syncSchemaData();

            return true;

        }

        $this->log('Starting database migration process.');

        if (!$current_version && $version == $schema['version']) {

            /**
             * This section sets up the database using the existing schema without migration replay.
             *
             * The criteria here is:
             *
             * * No current version
             * * $version must equal the schema file version
             *
             * Otherwise we have to replay the migration files from current version to the target version.
             */

            if (count($this->listTables()) > 1)
                throw new \Exception("Tables exist in database but no schema info was found!  This should only be run on an empty database!");

            /*
             * There is no current database so just initialise from the schema file.
             */
            $this->log("Initialising database" . ($version ? " at version '$version'" : ''));

            if ($schema['version'] > 0){

                if($test || $this->createSchema($schema))
                    $this->insert('schema_info', array('version' => $schema['version']));

            }

        } else {

            if (!array_key_exists($current_version, $versions))
                throw new \Exception("Your current database version has no migration source.");

            $this->log("Migrating from version '$current_version' to '$version'.");

            if ($version < $current_version) {

                $mode = 'down';

                krsort($versions);

            }

            $source = reset($versions);

            $this->log("Migrating $mode");

            do {

                $ver = key($versions);

                /**
                 * Break out once we get to the end of versions
                 */
                if (($mode == 'up' && ($ver > $version || $ver <= $current_version)) || ($mode == 'down' && ($ver <= $version || $ver > $current_version)))
                    continue;

                if ($mode == 'up') {

                    $this->log("--> Replaying version '$ver' from file '$source'.");

                } elseif ($mode == 'down') {

                    $this->log("<-- Rolling back version '$ver' from file '$source'.");

                } else {

                    throw new \Exception("Unknown mode!");

                }

                if (!($current_schema = json_decode($source->get_contents(), true)))
                    throw new \Exception("Unable to parse the migration file.  Bad JSON?");

                try{

                    $this->beginTransaction();

                    $this->replay($current_schema[$mode], $test);

                    if ($mode == 'up') {

                        $this->log('Inserting version record: ' . $ver);

                        if (!$test)
                            $this->insert('schema_info', array('version' => $ver));

                    } elseif ($mode == 'down') {

                        $this->log('Removing version record: ' . $ver);

                        if (!$test)
                            $this->delete('schema_info', array('version' => $ver));

                    }

                    $this->commit();

                }
                catch(\Exception $e){

                    $this->rollBack();

                    $this->log($e->getMessage());

                    return false;

                }

                $this->log("-- Replay of version '$ver' completed.");

            } while($source = next($versions));

        }

        //Insert data records.  Will only happen in an up migration.
        if($mode == 'up'){

            if(!$this->syncSchemaData(null, $test))
                return false;

        }

        $this->log('Migration completed successfully.');

        return true;

    }

    /**
     * Takes a schema definition and creates it in the database.
     *
     * @param array $schema
     */
    public function createSchema($schema){

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        $this->beginTransaction();

        try{

            /* Create tables */
            if($tables = ake($schema, 'tables')){

                foreach($tables as $table => $columns){

                    $ret = $this->createTable($table, $columns);

                    if(!$ret || $this->errorCode() > 0)
                        throw new \Exception('Error creating table ' . $table . ': ' . $this->errorInfo()[2]);

                }

            }

            /* Create foreign keys */
            if($constraints = ake($schema, 'constraints')){

                //Do primary keys first
                foreach($constraints as $table => $table_constraints){

                    foreach($table_constraints as $constraint_name => $constraint){

                        if($constraint['type'] !== 'PRIMARY KEY')
                            continue;

                        $ret = $this->addConstraint($constraint_name, $constraint);

                        if(!$ret || $this->errorCode() > 0)
                            throw new \Exception('Error creating constraint ' . $constraint_name . ': ' . $this->errorInfo()[2]);

                    }

                }

                //Now do all other constraints
                foreach($constraints as $table => $table_constraints){

                    foreach($table_constraints as $constraint_name => $constraint){

                        if($constraint['type'] == 'PRIMARY KEY')
                            continue;

                        $ret = $this->addConstraint($constraint_name, $constraint);

                        if(!$ret || $this->errorCode() > 0)
                            throw new \Exception('Error creating constraint ' . $constraint_name . ': ' . $this->errorInfo()[2]);

                    }

                }

            }

            /* Create indexes */
            if($indexes = ake($schema, 'indexes')){

                foreach($indexes as $table => $table_indexes){

                    foreach($table_indexes as $index_name => $index_info){

                        $ret = $this->createIndex($index_name, $table, $index_info);

                        if(!$ret || $this->errorCode() > 0)
                            throw new \Exception('Error creating index ' . $index_name . ': ' . $this->errorInfo()[2]);

                    }

                }

            }

        }
        catch(\Exception $e){

            $this->rollBack();

            throw $e;

        }

        $this->commit();

        return true;

    }

    /**
     * Reply a database migration schema file
     *
     * This should only be used internally by the migrate method to replay an individual schema migration file.
     *
     * @param array $schema
     *            The JSON decoded schema to replay.
     */
    private function replay($schema, $test = false) {

        if(!$this->driver)
            throw new Exception\DriverNotSpecified();

        foreach($schema as $action => $data) {

            if($action == 'data'){

                if(!$test)
                    $this->syncSchemaData($data);

            }else{

                foreach($data as $type => $items) {

                    foreach($items as $item_name => $item) {

                        switch ($action) {

                            case 'create' :

                                if ($type == 'table'){

                                    $this->log("+ Creating table '$item[name]'.");

                                    if ($test)
                                        continue;

                                    $this->createTable($item['name'], $item['cols']);

                                }elseif($type == 'index'){

                                    $this->log("+ Creating index '$item[name]' on table '$item[table]'.");

                                    if ($test)
                                        continue;

                                    $this->createIndex($item['name'], $item['table'], array('columns' => $item['columns'], 'unique' => $item['unique']));

                                }elseif($type == 'constraint'){

                                    $this->log("+ Creating constraint '$item[name]' on table '$item[table]'.");

                                    if ($test)
                                        continue;

                                    $this->addConstraint($item['name'], $item);

                                }else
                                    $this->log("I don't know how to create a {$type}!");

                                break;

                            case 'remove' :

                                if ($type == 'table'){

                                    $this->log("- Removing table '$item'.");

                                    if ($test)
                                        continue;

                                    $this->dropTable($item, true);

                                }elseif($type == 'constraint'){

                                    $this->log("- Removing constraint '$item[name]' from table '$item[table]'.");

                                    if ($test)
                                        continue;

                                    $this->dropConstraint($item['name'], $item['table'], true);

                                }elseif($type == 'index'){

                                    $this->log("- Removing index '$item'.");

                                    if ($test)
                                        continue;

                                    $this->dropIndex($item);

                                }else
                                    $this->log("I don't know how to remove a {$type}!");

                                break;

                            case 'alter' :

                                $this->log("> Altering $type $item_name");

                                if ($test)
                                    continue;

                                if ($type == 'table') {

                                    foreach($item as $alter_action => $columns) {

                                        foreach($columns as $col_name => $col) {

                                            if ($alter_action == 'add') {

                                                $this->log("+ Adding column '$col[name]'.");

                                                if ($test)
                                                    continue;

                                                $this->addColumn($item_name, $col);

                                            } elseif ($alter_action == 'alter') {

                                                $this->log("> Altering column '$col_name'.");

                                                if($test)
                                                    continue;

                                                $this->alterColumn($item_name, $col_name, $col);

                                            } elseif ($alter_action == 'drop') {

                                                $this->log("- Dropping column '$col'.");

                                                if ($test)
                                                    continue;

                                                $this->dropColumn($item_name, $col);

                                            }

                                            if($this->errorCode() > 0)
                                                throw new \Exception($this->errorInfo()[2]);

                                        }

                                    }

                                } else {

                                    $this->log("I don't know how to alter a {$type}!");

                                }

                                break;

                            case 'rename' :

                                $this->log("> Renaming $type item: $item[from] => $item[to]");

                                if ($test)
                                    continue;

                                if ($type == 'table')
                                    $this->renameTable($item['from'], $item['to']);

                                else
                                    $this->log("I don't know how to rename a {$type}!");

                                break;

                            default :
                                $this->log("I don't know how to $action a {$type}!");

                                break;

                        }

                        if($this->errorCode() > 0)
                            throw new \Exception($this->errorInfo()[2]);

                    }

                }

            }

        }

        return !$test;

    }

    public function syncSchemaData($data_schema = null, $test = false){

        $this->log("Initialising DBI data sync");

        $this->beginTransaction();

        if($data_schema === null){

            $data_schema = array();

            $this->loadDataFromFile($data_schema, $this->schema_file, 'data');

            $this->loadDataFromFile($data_schema, $this->data_file);

        }

        $this->log("Starting DBI data sync");

        foreach($data_schema as $info)
            $this->processDataObject($info);

        if($test)
            $this->rollBack();
        else
            $this->commit();

        $this->log('DBI Data sync Completed');

        if(method_exists($this->driver, 'repair')){

            $this->log('Running ' . $this->driver . ' repair process');

            $result = $this->driver->repair();

            $this->log('Repair ' . ($result?'completed successfully':'failed'));

        }

        return true;

    }

    private function loadDataFromFile(&$data_schema, $file, $child_element = null){

        if(!$file instanceof \Hazaar\File)
            $file = new \Hazaar\File($file);

        if (!$file->exists())
            return;

        $this->log('Loading data from file: ' . $file);

        if (!($data = json_decode($file->get_contents(), true)))
            throw new \Exception("Unable to parse the DBI data file.  Bad JSON?");

        if($child_element)
            $data = ake($data, $child_element);

        if(!is_array($data))
            return;

        foreach($data as &$item){

            if(is_string($item))
                $this->loadDataFromFile($data_schema, $file->dirname() . DIRECTORY_SEPARATOR . ltrim($item, DIRECTORY_SEPARATOR));
            else
                $data_schema[] = $item;

        }

    }

    private function processDataObject($info){

        if(!is_array($info))
            throw new \Exception('Got non-array while processing data object!');

        if($message = ake($info, 'message'))
            $this->log($message);

        if(($table = ake($info, 'table'))){

            //The 'rows' element is used to synchronise table rows in the database.
            if($rows = ake($info, 'rows')){

                if($this->describeTable($table) == false)
                    throw new \Exception("Can not insert rows into non-existant table '$table'!");

                $pkey = null;

                if($constraints = $this->listConstraints($table, 'PRIMARY KEY')){

                    $pkey = ake(reset($constraints), 'column');

                }else{

                    throw new \Exception("Can not migrate data on table '$table' without primary key!");

                }

                $this->log("Processing " . count($rows) . " records in table '$table'");

                foreach($rows as $id => $row){

                    $do_diff = false;

                    /**
                     * If the primary key is in the record, find the record using only that field, then
                     * we will check for differences between the records
                     */
                    if(array_key_exists($pkey, $row)){

                        $criteria = array($pkey => $row[$pkey]);

                        $do_diff = true;

                    }else{ //Otherwise, look for the record in it's entirity and only insert if it doesn't exist.

                        $criteria = $row;

                    }

                    if($current = $this->table($table)->findOne($criteria)){

                        //If this is an insert only row then move on because this row exists
                        if(ake($info, 'insertonly'))
                            continue;

                        if($do_diff){

                            $diff = array_diff_assoc_recursive($row, $current);

                            if(count($diff) > 0){

                                $this->log("Updating record in table '$table' with $pkey={$row[$pkey]}");

                                foreach($row as &$col) if(is_array($col)) $col = array('$array' => $col);

                                if(!$this->update($table, $row, array($pkey => $row[$pkey])))
                                    throw new \Exception('Update failed: ' . $this->errorInfo()[2]);

                            }

                        }

                    }else{

                        //If this is an update only row then move on because this row does not exist
                        if(ake($info, 'updateonly'))
                            continue;

                        foreach($row as &$col) if(is_array($col)) $col = array('$array' => $col);

                        if(($pkey_value = $this->insert($table, $row, $pkey)) == false)
                            throw new \Exception('Insert failed: ' . $this->errorInfo()[2]);

                        $this->log("Inserted record into table '$table' with $pkey={$pkey_value}");

                    }

                }

            }

            //The 'update' element is used to trigger updates on existing rows in a database
            if($updates = ake($info, 'update')){

                foreach($updates as $update){

                    if(!($where = ake($update, 'where')) && ake($update, 'all', false) !== true)
                        throw new \Exception("Can not update rows in a table without a 'where' element or setting 'all=true'.");

                    $affected = $this->table($table)->update($where, ake($update, 'set'));

                    if($affected === false)
                        throw new \Exception('Update failed: ' . $this->errorInfo()[2]);

                    $this->log("Updated $affected rows");

                }

            }

            //The 'delete' element is used to remove existing rows in a database table
            if($deletes = ake($info, 'delete')){

                foreach($deletes as $delete){

                    if(ake($delete, 'all', false) === true){

                        $affected = $this->table($table)->deleteAll();

                    }else{

                        if(!($where = ake($delete, 'where')))
                            throw new \Exception("Can not delete rows from a table without a 'where' element or setting 'all=true'.");

                        $affected = $this->table($table)->delete($where);

                    }

                    if($affected === false)
                        throw new \Exception('Delete failed: ' . $this->errorInfo()[2]);

                    $this->log("Deleted $affected rows");

                }

            }

        }

    }

    /**
     * Logs a message to the migration log.
     *
     * @param string $msg
     *            The message to log.
     */
    private function log($msg) {

        $this->migration_log[] = array(
            'time' => microtime(true),
            'msg' => $msg
        );

    }

    /**
     * Returns the migration log
     *
     * Snapshots and migrations are complex processes where many things happen in a single execution. This means stuff
     * can go wrong and you will probably want to know what/why when they do.
     *
     * When running \Hazaar\Adapter::snapshot() or \Hazaar\Adapter::migrate() a log of what has been done is stored internally
     * in an array of timestamped messages. You can use the \Hazaar\Adapter::getMigrationLog() method to retrieve this
     * log so that if anything goes wrong, you can see what and fix it/
     */
    public function getMigrationLog() {

        return $this->migration_log;

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

}
