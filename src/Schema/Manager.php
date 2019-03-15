<?php

/**
 * @file        Hazaar/DBI/SchemaManager.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2018 Jamie Carl (http://www.hazaarlabs.com)
 */
namespace Hazaar\DBI\Schema;

use \Hazaar\DBI\Adapter;

/**
 * Relational Database Schema Manager
 *
 */
class Manager {

    private $dbi;

    private $schema_file;

    private $data_file;

    private $migration_log = array();

    static public $schema_info_table = 'schema_info';

    private $ignore_tables = array('schema_info', 'hz_file', 'hz_file_chunk');

    function __construct(Adapter $dbi) {

        $this->dbi = $dbi;

        $this->schema_file = realpath(APPLICATION_PATH . DIRECTORY_SEPARATOR . '..')
            . DIRECTORY_SEPARATOR . 'db' . DIRECTORY_SEPARATOR . 'schema.json';

        $this->data_file = realpath(APPLICATION_PATH . DIRECTORY_SEPARATOR . '..')
            . DIRECTORY_SEPARATOR . 'db' . DIRECTORY_SEPARATOR . 'data.json';

    }

    public function getVersion() {

        if (!$this->dbi->schema_info->exists())
            return false;

        $result = $this->dbi->schema_info->findOne(array(), array('version' => "max(version)"));

        return ake($result, 'version', false);

    }

    public function getVersions($with_file_obj = false) {

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

    public function getLatestVersion($with_file_obj = false){

        $versions = $this->getVersions($with_file_obj);

        end($versions);

        return key($versions);

    }

    public function isLatest(){

        return $this->getLatestVersion() === $this->getVersion();

    }

    /**
     * Creates the info table that stores the version info of the current database.
     */
    private function createInfoTable() {

        if (!$this->dbi->tableExists(Manager::$schema_info_table)) {

            $this->dbi->createTable(Manager::$schema_info_table, array(
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
     * This method is used to create the database schema migration files. These files are used by
     * the \Hazaar\Adapter::migrate() method to bring a database up to a certain version. Using this method
     * simplifies creating these migration files and removes the need to create them manually when there are
     * trivial changes.
     *
     * When developing your project
     *
     * Currently only the following changes are supported:
     * * Table creation, removal and rename.
     * * Column creation, removal and alteration.
     * * Index creation and removal.
     *
     * !!! notice
     *
     * Table rename detection works by comparing new tables with removed tables for tables that have the same
     * columns. Because of this, rename detection will not work if columns are added or removed at the same time
     * the table is renamed. If you want to rename a table, make sure that this is the only operation being
     * performed on the table for a single snapshot. Modifying other tables will not affect this. If you want to
     * rename a table AND change it's column layout, make sure you do either the rename or the modifications
     * first, then snapshot, then do the other operation before snapshotting again.
     *
     * @param string $comment A comment to add to the migration file.
     *
     * @throws \Exception
     *
     * @return boolean True if the snapshot was successful. False if no changes were detected and nothing needed to be done.
     */
    public function snapshot($comment = null, $test = false) {

        $this->log('Snapshot process starting');

        if ($test)
            $this->log('Test mode ENABLED');

        if ($versions = $this->getVersions()) {

            end($versions);

            $latest_version = key($versions);

        } else {

            $latest_version = 0;

        }

        $version = $this->getVersion();

        if ($latest_version > $version)
            throw new \Exception('Snapshoting a database that is not at the latest schema version is not supported.');

        $this->dbi->beginTransaction();

        $db_dir = dirname($this->schema_file);

        if (!is_dir($db_dir)) {

            if (file_exists($db_dir))
                throw new \Exception('Unable to create database migration directory.  It exists but is not a directory!');

            mkdir($db_dir);

        }

        try {

            $result = $this->dbi->query('SELECT CURRENT_TIMESTAMP');

            if (!$result instanceof \Hazaar\DBI\Result)
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
         * Stores only changes between $schema and $current_schema.  Here we define all possible elements
         * to ensure the correct ordering.  Later we remove all empty elements before saving the migration file.
         */
        $changes = array(
            "version" => 2,
            'up' => array(
                'table' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array(),
                    'rename' => array()
                ),
                'constraint' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'index' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'view' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'function' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                )
            ),
            'down' => array(
                'raise' => array(),
                'function' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'view' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'index' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'constraint' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array()
                ),
                'table' => array(
                    'create' => array(),
                    'alter' => array(),
                    'remove' => array(),
                    'rename' => array()
                )
            )
        );

        if($init)
            $changes['down']['raise'] = 'Can not revert initial snapshot';

        /**
         * Check for any new tables or changes to existing tables.
         * This pretty much looks just for tables to add and
         * any columns to alter.
         */
        foreach($this->dbi->listTables() as $table) {

            $name = $table['name'];

            if (in_array($name, $this->ignore_tables))
                continue;

            $this->log("Processing table '$name'.");

            if(!($cols = $this->dbi->describeTable($name, 'ordinal_position')))
                throw new \Exception("Error getting table definition for table '$name'.  Does the connected user have the correct permissions?");

            $current_schema['tables'][$name] = $cols;

            //BEGIN PROCESSING TABLES
            if (array_key_exists('tables', $schema) && array_key_exists($name, $schema['tables'])) {

                $this->log("Table '$name' already exists.  Checking differences.");

                $diff = $this->getTableDiffs($cols, $schema['tables'][$name]);

                if (count($diff) > 0) {

                    $this->log("> Table '$name' has changed.");

                    $changes['up']['table']['alter'][$name] = $diff;

                    foreach($diff as $diff_mode => $col_diff) {

                        $diff_mode = ($diff_mode == 'add') ? 'drop' : 'add';

                        foreach($col_diff as $col_name => $col_info) {

                            if ($diff_mode == 'add') {

                                $info = $this->getColumn($col_info, $schema['tables'][$name]);

                                $changes['down']['table']['alter'][$name][$diff_mode][$col_name] = $info;

                            } else {

                                $changes['down']['table']['alter'][$name][$diff_mode][] = $col_name;

                            }

                        }

                    }

                } else {

                    $this->log("No changes to table '$name'.");

                }

            } else { // Table doesn't exist, so we add a command to create the whole thing

                $this->log("+ Table '$name' has been created.");

                $changes['up']['table']['create'][] = array(
                    'name' => $name,
                    'cols' => $cols
                );

                if (!$init)
                    $changes['down']['table']['remove'][] = $name;

            } //END PROCESSING TABLES

            //BEGIN PROCESSING CONSTRAINTS
            $constraints = $this->dbi->listConstraints($name);

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

                        $changes['up']['constraint']['create'][] = array_merge($constraint, array(
                            'name' => $constraint_name,
                        ));

                        //If the constraint was added at the same time as the table, we don't need to add the removes
                        if(! $init && !(array_key_exists('down', $changes)
                            && array_key_exists('table', $changes['down'])
                            && array_key_exists('remove', $changes['down']['table'])
                            && in_array($name, $changes['down']['table']['remove'])))

                            $changes['down']['constraint']['remove'][] = array('table' => $name, 'name' => $constraint_name);

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

                        $changes['up']['constraint']['remove'][] = array('table' => $name, 'name' => $constraint);

                        $changes['down']['constraint']['create'][] = array_merge($idef, array(
                            'name' => $constraint,
                            'table' => $name
                        ));

                    }

                }

            }elseif(count($constraints) > 0){

                foreach($constraints as $constraint_name => $constraint){

                    $this->log("+ Added new constraint '$constraint_name' on table '$name'.");

                    $changes['up']['constraint']['create'][] = array_merge($constraint, array(
                        'name' => $constraint_name,
                    ));

                    if (!$init)
                        $changes['down']['constraint']['remove'][] = array('table' => $name, 'name' => $constraint_name);

                }

            } //END PROCESSING CONSTRAINTS

            //BEGIN PROCESSING INDEXES
            $indexes = $this->dbi->listIndexes($name);

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

                    $changes['up']['index']['create'][] = $index;

                    if(!$init)
                        $changes['down']['index']['remove'][] = $index_name;

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

                        $changes['up']['index']['remove'][] = $index;

                        $changes['down']['index']['create'][] = array_merge($idef, array(
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

                    $changes['up']['index']['create'][] = array_merge($index, array(
                        'name' => $index_name,
                        'table' => $name,
                    ));

                    if (!$init)
                        $changes['down']['index']['remove'][] = $index_name;

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

                    $changes['up']['table']['remove'][] = $table;

                    $changes['down']['table']['create'][] = array(
                        'name' => $table,
                        'cols' => $schema['tables'][$table]
                    );

                    //Add any constraints that were on this table to the down script so they get re-created
                    if(array_key_exists('constraints', $schema)
                        && array_key_exists($table, $schema['constraints'])){

                        $changes['down']['constraint']['create'] = array();

                        foreach($schema['constraints'][$table] as $constraint_name => $constraint){

                            $changes['down']['constraint']['create'][] = array_merge($constraint, array(
                                'name' => $constraint_name,
                                'table' => $table
                            ));

                        }

                    }

                    //Add any indexes that were on this table to the down script so they get re-created
                    if(array_key_exists('indexes', $schema)
                        && array_key_exists($table, $schema['indexes'])){

                        $changes['down']['index']['create'] = array();

                        foreach($schema['indexes'][$table] as $index_name => $index){

                            $changes['down']['index']['create'][] = array_merge($index, array(
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
        if (isset($changes['up']['table']['create']) && isset($changes['up']['table']['remove'])) {

            $this->log('Looking for renamed tables.');

            foreach($changes['up']['table']['create'] as $create_key => $create) {

                foreach($changes['up']['table']['remove'] as $remove_key => $remove) {

                    $diff = array_udiff($schema['tables'][$remove], $create['cols'], function($a, $b){
                        if($a['name'] == $b['name']) return 0;
                        return (($a['name'] > $b['name']) ? 1: -1);
                    });

                    if(!$diff){

                        $this->log("> Table '$remove' has been renamed to '{$create['name']}'.", LOG_NOTICE);

                        $changes['up']['table']['rename'][] = array(
                            'from' => $remove,
                            'to' => $create['name']
                        );

                        $changes['down']['table']['rename'][] = array(
                            'from' => $create['name'],
                            'to' => $remove
                        );

                        /**
                         * Clean up the changes
                         */
                        $changes['up']['table']['create'][$create_key] = null;

                        $changes['up']['table']['remove'][$remove_key] = null;

                        foreach($changes['down']['table']['remove'] as $down_remove_key => $down_remove) {

                            if ($down_remove === $create['name'])
                                $changes['down']['table']['remove'][$down_remove_key] = null;

                        }

                        foreach($changes['down']['table']['create'] as $down_create_key => $down_create) {

                            if ($down_create['name'] == $remove)
                                $changes['down']['table']['create'][$down_create_key] = null;

                        }

                    }

                }

            }

        }

        //BEGIN PROCESSING VIEWS
        foreach($this->dbi->listViews() as $view){

            $name = $view['name'];

            $this->log("Processing view '$name'.");

            if(!($info = $this->dbi->describeView($name)))
                throw new \Exception("Error getting view definition for view '$name'.  Does the connected user have the correct permissions?");

            $current_schema['views'][$name] = $info;

            if (array_key_exists('views', $schema) && array_key_exists($name, $schema['views'])) {

                $this->log("View '$name' already exists.  Checking differences.");

                $diff = array_diff_assoc($schema['views'][$name], $info);

                if (count($diff) > 0) {

                    $this->log("> View '$name' has changed.");

                    $changes['up']['view']['alter'][$name] = $info;

                    $changes['down']['view']['alter'][$name] = $schema['views'][$name];

                } else {

                    $this->log("No changes to view '$name'.");

                }

            } else { // View doesn't exist, so we add a command to create the whole thing

                $this->log("+ View '$name' has been created.");

                $changes['up']['view']['create'][] = $info;

                if (!$init)
                    $changes['down']['view']['remove'][] = $name;

            }

        } //END PROCESSING VIEWS

        //BEGIN PROCESSING FUNCTIONS
        foreach($this->dbi->listFunctions() as $func){

            $name = $func['name'];

            $this->log("Processing function '$name'.");

            if(!($infos = $this->dbi->describeFunction($name)))
                throw new \Exception("Error getting function definition for functions '$name'.  Does the connected user have the correct permissions?");

            foreach($infos as $info){

                $current_schema['functions'][$name][] = $info;

                $params = array();

                foreach($info['parameters'] as $p) $params[] = $p['type'];

                $fullname = $name . '(' . implode(', ', $params) . ')';

                if (array_key_exists('functions', $schema)
                    && array_key_exists($name, $schema['functions'])
                && count($ex_info = array_filter($schema['functions'][$name], function($item) use($info){
                        if(count($item['parameters']) !== count($info['parameters'])) return false;
                        foreach($item['parameters'] as $i => $p)
                            if(!(array_key_exists($i, $info['parameters']) && $info['parameters'][$i]['type'] === $p['type']))
                                return false;
                        return true;
                    })) > 0) {

                    $this->log("Function '$fullname' already exists.  Checking differences.");

                    foreach($ex_info as $e){

                        $diff = array_diff_assoc_recursive($info, $e);

                        if (count($diff) > 0) {

                            $this->log("> Function '$fullname' has changed.");

                            $changes['up']['function']['alter'][] = $info;

                            $changes['down']['function']['alter'][] = $e;

                        } else {

                            $this->log("No changes to function '$fullname'.");

                        }

                    }

                } else { // View doesn't exist, so we add a command to create the whole thing

                    $this->log("+ Function '$fullname' has been created.");

                    $changes['up']['function']['create'][] = $info;

                    if (!$init)
                        $changes['down']['function']['remove'][] = array('name' => $name, 'parameters' => $params);

                }

            }

        } //END PROCESSING FUNCTIONS

        array_remove_empty($changes);

        //If there are no changes, bail out now
        if(!(count(ake($changes, 'up', array())) + count(ake($changes, 'down', array()))) > 0){

            $this->log('No changes detected.');

            $this->dbi->rollback();

            return false;

        }

        if(array_key_exists('up', $changes)){

            $tokens = array('create' => '+', 'alter' => '>', 'remove' => '-');

            foreach($changes['up'] as $type => $methods)
                foreach($methods as $method => $actions)
                    $this->log($tokens[$method] . ' ' . ucfirst($method) . ' ' . $type . ' count: ' . count($actions));

        }

        //If we are testing, then return the diff between the previous schema version
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

        $this->dbi->insert('schema_info', array(
            'version' => $version
        ));

        $this->dbi->commit();

        return true;

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

        $this->log('Migration process starting');

        if ($test)
            $this->log('Test mode ENABLED');

        $mode = 'up';

        $current_version = 0;

        $versions = $this->getVersions(true);

        $file = new \Hazaar\File($this->schema_file);

        if (!$file->exists())
            throw new \Exception("This application has no schema file.  Database schema is not being managed.");

        if (!($schema = json_decode($file->get_contents(), true)))
            throw new \Exception("Unable to parse the migration file.  Bad JSON?");

        if(!array_key_exists('version', $schema))
            $schema['version'] = 1;

        if ($version) {

            if(!is_string($version))
                settype($version, 'string');

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

            if ($e->getCode() == 7)
                throw new \Exception("Database does not exist.");

            throw new \Exception($e->getMessage());

        }

        /**
         * Get the current version (if any) from the database
         */
        if($this->dbi->tableExists('schema_info')){

            if ($result = $this->dbi->table('schema_info')->find(array(), array('version'))->sort('version', true)) {

                if($row = $result->fetch())
                    $current_version = $row['version'];

                $this->log("Current database version: " . ($current_version ? $current_version : "None"));

            }

        }

        /**
         * Check to see if we are at the current version first.
         */
        if ($current_version === $version) {

            $this->log("Database is already at version: $version");

        }else{

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

                $tables = $this->dbi->listTables();

                $excluded = $this->ignore_tables;

                $tables = array_filter($tables, function($value) use($excluded){
                    return !($value['schema'] === 'public' && in_array($value['name'], $excluded));
                });

                if (count($tables) > 0){

                    $this->log("Tables exist in database but no schema info was found!  This should only be run on an empty database!");

                }else{

                    /*
                     * There is no current database so just initialise from the schema file.
                     */
                    $this->log("Initialising database" . ($version ? " at version '$version'" : ''));

                    if ($schema['version'] > 0){

                        if($test || $this->createSchema($schema)){

                            foreach($versions as $ver => $name)
                                $this->dbi->insert('schema_info', array('version' => $ver));

                        }

                    }

                    $force_data_sync = true;

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

                        $this->dbi->beginTransaction();

                        $this->replay($current_schema[$mode], $test, ake($current_schema, 'version', 1));

                        if ($mode == 'up') {

                            $this->log('Inserting version record: ' . $ver);

                            if (!$test)
                                $this->dbi->insert('schema_info', array('version' => $ver));

                        } elseif ($mode == 'down') {

                            $this->log('Removing version record: ' . $ver);

                            if (!$test)
                                $this->dbi->delete('schema_info', array('version' => $ver));

                        }

                        if($this->dbi->errorCode() > 0)
                            throw new \Exception($this->dbi->errorInfo()[2]);

                        $this->dbi->commit();

                    }
                    catch(\Exception $e){

                        $this->dbi->rollBack();

                        $this->log($e->getMessage());

                        return false;

                    }

                    $this->log("-- Replay of version '$ver' completed.");

                } while($source = next($versions));

                if($mode === 'up')
                    $force_data_sync = true;

            }

        }

        $this->initDBIFilesystem();

        //Insert data records.  Will only happen in an up migration.
        if($force_data_sync){

            if(!$this->syncData(null, $test))
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

        if(!\Hazaar\Map::is_array($schema))
            return false;

        $this->dbi->beginTransaction();

        try{

            /* Create tables */
            if($tables = ake($schema, 'tables')){

                foreach($tables as $table => $columns){

                    $ret = $this->dbi->createTable($table, $columns);

                    if(!$ret || $this->dbi->errorCode() > 0)
                        throw new \Exception('Error creating table ' . $table . ': ' . $this->dbi->errorInfo()[2]);

                }

            }

            /* Create foreign keys */
            if($constraints = ake($schema, 'constraints')){

                //Do primary keys first
                foreach($constraints as $table => $table_constraints){

                    foreach($table_constraints as $constraint_name => $constraint){

                        if($constraint['type'] !== 'PRIMARY KEY')
                            continue;

                        $ret = $this->dbi->addConstraint($constraint_name, $constraint);

                        if(!$ret || $this->dbi->errorCode() > 0)
                            throw new \Exception('Error creating constraint ' . $constraint_name . ': ' . $this->dbi->errorInfo()[2]);

                    }

                }

                //Now do all other constraints
                foreach($constraints as $table => $table_constraints){

                    foreach($table_constraints as $constraint_name => $constraint){

                        if($constraint['type'] == 'PRIMARY KEY')
                            continue;

                        $ret = $this->dbi->addConstraint($constraint_name, $constraint);

                        if(!$ret || $this->dbi->errorCode() > 0)
                            throw new \Exception('Error creating constraint ' . $constraint_name . ': ' . $this->dbi->errorInfo()[2]);

                    }

                }

            }

            /* Create indexes */
            if($indexes = ake($schema, 'indexes')){

                foreach($indexes as $table => $table_indexes){

                    foreach($table_indexes as $index_name => $index_info){

                        $ret = $this->dbi->createIndex($index_name, $table, $index_info);

                        if(!$ret || $this->dbi->errorCode() > 0)
                            throw new \Exception('Error creating index ' . $index_name . ': ' . $this->dbi->errorInfo()[2]);

                    }

                }

            }

            /* Create views */
            if($views = ake($schema, 'views')){

                foreach($views as $view => $info){

                    $ret = $this->dbi->createView($view, $info['content']);

                    if(!$ret || $this->dbi->errorCode() > 0)
                        throw new \Exception('Error creating view ' . $view . ': ' . $this->dbi->errorInfo()[2]);

                }

            }

            /* Create functions */
            if($functions = ake($schema, 'functions')){

                foreach($functions as $items){

                    foreach($items as $info){

                        $params = array();

                        foreach($info['parameters'] as $p) $params[] = $p['type'];

                        $ret = $this->dbi->createFunction($info['name'], $info);

                        if(!$ret || $this->dbi->errorCode() > 0)
                            throw new \Exception('Error creating function ' . $info['name'] . '(' . implode(', ', $params) . '): ' . $this->dbi->errorInfo()[2]);

                    }

                }

            }

        }
        catch(\Exception $e){

            $this->dbi->rollBack();

            throw $e;

        }

        $this->dbi->commit();

        return true;

    }

    public function createSchemaFromFile($filename){

        if(!$filename = realpath($filename))
            throw new \Exception('Schema file not found!', 404);

        if(!($schema = json_decode(file_get_contents($filename), true)))
            throw new \Exception('Schema file contents is not a valid schema!');

        return $this->createSchema($schema);

    }

    /**
     * Reply a database migration schema file
     *
     * This should only be used internally by the migrate method to replay an individual schema migration file.
     *
     * @param array $schema
     *            The JSON decoded schema to replay.
     */
    private function replay($schema, $test = false, $version = 1) {

        foreach($schema as $level1 => $data) {

            if($level1 == 'data'){

                if(!$test)
                    $this->syncData($data);

                continue;

            }

            foreach($data as $level2 => $items) {

                if($version === 1)
                    $this->replayItems($level2, $level1, $items, $test);
                elseif($version === 2)
                    $this->replayItems($level1, $level2, $items, $test);
                else
                    throw new \Exception('Unsupported schema migration version: ' . $version);

            }

        }

        return !$test;

    }

    private function replayItems($type, $action, $items, $test = false){

        foreach($items as $item_name => $item) {

            switch ($action) {

                case 'create' :

                    if ($type === 'table'){

                        $this->log("+ Creating table '$item[name]'.");

                        if ($test)
                            continue;

                        $this->dbi->createTable($item['name'], $item['cols']);

                    }elseif($type === 'index'){

                        $this->log("+ Creating index '$item[name]' on table '$item[table]'.");

                        if ($test)
                            continue;

                        $this->dbi->createIndex($item['name'], $item['table'], array('columns' => $item['columns'], 'unique' => $item['unique']));

                    }elseif($type === 'constraint'){

                        $this->log("+ Creating constraint '$item[name]' on table '$item[table]'.");

                        if ($test)
                            continue;

                        $this->dbi->addConstraint($item['name'], $item);

                    }elseif($type === 'view'){

                        $this->log("+ Creating view '$item[name]'.");

                        if ($test)
                            continue;

                        $this->dbi->createView($item['name'], $item['content']);

                    }elseif($type === 'function'){

                        $params = array();

                        foreach($item['parameters'] as $p) $params[] = $p['type'];

                        $this->log("+ Creating function '{$item['name']}(" . implode(', ', $params) . ').');

                        if ($test)
                            continue;

                        $this->dbi->createFunction($item['name'], $item);

                    }else
                        $this->log("I don't know how to create a {$type}!");

                    break;

                case 'remove' :

                    if ($type === 'table'){

                        $this->log("- Removing table '$item'.");

                        if ($test)
                            continue;

                        $this->dbi->dropTable($item, true);

                    }elseif($type === 'constraint'){

                        $this->log("- Removing constraint '$item[name]' from table '$item[table]'.");

                        if ($test)
                            continue;

                        $this->dbi->dropConstraint($item['name'], $item['table'], true);

                    }elseif($type === 'index'){

                        $this->log("- Removing index '$item'.");

                        if ($test)
                            continue;

                        $this->dbi->dropIndex($item);

                    }elseif($type === 'view'){

                        $this->log("- Removing view '$item'.");

                        if ($test)
                            continue;

                        $this->dbi->dropView($item, true);

                    }elseif($type === 'function'){

                        $this->log("- Removing function '{$item['name']}(" . implode(', ', $item['parameters']) . ').');

                        if ($test)
                            continue;

                        $this->dbi->dropFunction($item['name'], $item['parameters']);

                    }else
                        $this->log("I don't know how to remove a {$type}!");

                    break;

                case 'alter' :

                    $this->log("> Altering $type $item_name");

                    if ($test)
                        continue;

                    if ($type === 'table') {

                        foreach($item as $alter_action => $columns) {

                            foreach($columns as $col_name => $col) {

                                if ($alter_action == 'add') {

                                    $this->log("+ Adding column '$col[name]'.");

                                    if ($test)
                                        continue;

                                    $this->dbi->addColumn($item_name, $col);

                                } elseif ($alter_action == 'alter') {

                                    $this->log("> Altering column '$col_name'.");

                                    if($test)
                                        continue;

                                    $this->dbi->alterColumn($item_name, $col_name, $col);

                                } elseif ($alter_action == 'drop') {

                                    $this->log("- Dropping column '$col'.");

                                    if ($test)
                                        continue;

                                    $this->dbi->dropColumn($item_name, $col);

                                }

                                if($this->dbi->errorCode() > 0)
                                    throw new \Exception($this->dbi->errorInfo()[2]);

                            }

                        }

                    } elseif ($type === 'view'){

                        $this->dbi->dropView($item_name);

                        if($this->dbi->errorCode() > 0)
                            throw new \Exception($this->dbi->errorInfo()[2]);

                        $this->dbi->createView($item_name, $item['content']);

                    }elseif($type === 'function'){

                        $params = array();

                        foreach($item['parameters'] as $p) $params[] = $p['type'];

                        $this->log("+ Replacing function '{$item['name']}(" . implode(', ', $params) . ').');

                        if ($test)
                            continue;

                        $this->dbi->createFunction($item['name'], $item);

                    } else {

                        $this->log("I don't know how to alter a {$type}!");

                    }

                    break;

                case 'rename' :

                    $this->log("> Renaming $type item: $item[from] => $item[to]");

                    if ($test)
                        continue;

                    if ($type == 'table')
                        $this->dbi->renameTable($item['from'], $item['to']);

                    else
                        $this->log("I don't know how to rename a {$type}!");

                    break;

                default :
                    $this->log("I don't know how to $action a {$type}!");

                    break;

            }

            if($this->dbi->errorCode() > 0)
                throw new \Exception($this->dbi->errorInfo()[2]);

        }

        return true;

    }

    public function syncData($data_schema = null, $test = false){

        $this->log("Initialising DBI data sync");

        $this->dbi->beginTransaction();

        if($data_schema === null){

            $data_schema = array();

            $this->loadDataFromFile($data_schema, $this->schema_file, 'data');

            $this->loadDataFromFile($data_schema, $this->data_file);

        }

        $this->log("Starting DBI data sync");

        try{

            foreach($data_schema as $info)
                $this->processDataObject($info);

            if($test)
                $this->dbi->rollBack();
            else
                $this->dbi->commit();

            $this->log('DBI Data sync completed successfully!');

            if(method_exists($this->dbi->driver, 'repair')){

                $this->log('Running ' . $this->dbi->driver . ' repair process');

                $result = $this->dbi->driver->repair();

                $this->log('Repair ' . ($result?'completed successfully':'failed'));

            }

        }
        catch(\Throwable $e){

            $this->dbi->rollBack();

            $this->log('DBI Data sync error: ' . $e->getMessage());

        }

        return true;

    }

    private function loadDataFromFile(&$data_schema, $file, $child_element = null){

        if(!$file instanceof \Hazaar\File)
            $file = new \Hazaar\File($file);

        if (!$file->exists())
            return;

        $this->log('Loading data from file: ' . $file);

        if (!($data = json_decode($file->get_contents())))
            throw new \Exception("Unable to parse the DBI data file.  Bad JSON in $file");

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

        if(!$info instanceof \stdClass)
            throw new \Exception('Got non-object while processing data object!');

        if($message = ake($info, 'message'))
            $this->log($message);

        if(($table = ake($info, 'table'))){

            //The 'rows' element is used to synchronise table rows in the database.
            if($rows = ake($info, 'rows')){

                if(($def = $this->dbi->describeTable($table)) === false)
                    throw new \Exception("Can not insert rows into non-existant table '$table'!");

                $tableDef =  array_combine(array_column($def, 'name'), $def);

                $pkey = null;

                if($constraints = $this->dbi->listConstraints($table, 'PRIMARY KEY')){

                    $pkey = ake(reset($constraints), 'column');

                }else{

                    throw new \Exception("Can not migrate data on table '$table' without primary key!");

                }

                $this->log("Processing " . count($rows) . " records in table '$table'");

                //Quick closure function to fix up the row ready for insert/update
                $fix_row = function($row, $tableDef){

                    foreach($row as $name => &$col) {

                        if(!array_key_exists($name, $tableDef))
                            throw new \Exception("Attempting to modify data for non-existent row '$name'!" );

                        if($col === null) continue;

                        if(substr($tableDef[$name]['data_type'], 0, 4) === 'json')
                            $col = json_encode($col);
                        elseif(is_array($col))
                            $col = array('$array' => $col);

                    }

                    return $row;

                };

                foreach($rows as $row){

                    $do_diff = false;

                    /**
                     * If the primary key is in the record, find the record using only that field, then
                     * we will check for differences between the records
                     */
                    if(property_exists($row, $pkey)){

                        $criteria = array($pkey => ake($row, $pkey));

                        $do_diff = true;

                    }else{ //Otherwise, look for the record in it's entirity and only insert if it doesn't exist.

                        $criteria = (array)$row;

                    }

                    if($current = $this->dbi->table($table)->findOne($criteria)){

                        //If this is an insert only row then move on because this row exists
                        if(ake($info, 'insertonly'))
                            continue;

                        if(!$do_diff)
                            continue;

                        //If nothing has been added to the row, look for child arrays/objects to backwards analyse
                        if(count(array_diff_assoc_recursive($row, $current)) === 0){

                            $changes = 0;

                            foreach($row as $name => &$col) {

                                if(!(is_array($col) || $col instanceof \stdClass))
                                    continue;

                                $changes += count(array_diff_assoc_recursive(ake($current, $name), $col));

                            }

                            if($changes === 0)
                                continue;

                        }

                        $pkey_value = ake($row, $pkey);

                        $this->log("Updating record in table '$table' with $pkey={$pkey_value}");

                        if(!$this->dbi->update($table, $fix_row($row, $tableDef), array($pkey => $pkey_value)))
                            throw new \Exception('Update failed: ' . $this->dbi->errorInfo()[2]);

                    }else{

                        //If this is an update only row then move on because this row does not exist
                        if(ake($info, 'updateonly'))
                            continue;

                        if(($pkey_value = $this->dbi->insert($table, $fix_row($row, $tableDef), $pkey)) == false)
                            throw new \Exception('Insert failed: ' . $this->dbi->errorInfo()[2]);

                        $this->log("Inserted record into table '$table' with $pkey={$pkey_value}");

                    }

                }

            }

            //The 'update' element is used to trigger updates on existing rows in a database
            if($updates = ake($info, 'update')){

                foreach($updates as $update){

                    if(!($where = ake($update, 'where')) && ake($update, 'all', false) !== true)
                        throw new \Exception("Can not update rows in a table without a 'where' element or setting 'all=true'.");

                    $affected = $this->dbi->table($table)->update($where, ake($update, 'set'));

                    if($affected === false)
                        throw new \Exception('Update failed: ' . $this->dbi->errorInfo()[2]);

                    $this->log("Updated $affected rows");

                }

            }

            //The 'delete' element is used to remove existing rows in a database table
            if($deletes = ake($info, 'delete')){

                foreach($deletes as $delete){

                    if(ake($delete, 'all', false) === true){

                        $affected = $this->dbi->table($table)->deleteAll();

                    }else{

                        if(!($where = ake($delete, 'where')))
                            throw new \Exception("Can not delete rows from a table without a 'where' element or setting 'all=true'.");

                        $affected = $this->dbi->table($table)->delete($where);

                    }

                    if($affected === false)
                        throw new \Exception('Delete failed: ' . $this->dbi->errorInfo()[2]);

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

    private function initDBIFilesystem(){

        $config = new \Hazaar\Application\Config('media');

        foreach($config as $name => $settings){

            if($settings->get('type') !== 'DBI')
                continue;

            $fs_db = null;

            $this->log('Found DBI filesystem: ' . $name);

            try{

                $settings->enhance(array('dbi' => \Hazaar\DBI\Adapter::getDefaultConfig(), 'initialise' => true));

                $fs_db = new \Hazaar\DBI\Adapter($settings['dbi']);

                if($fs_db->tableExists('hz_file') && $fs_db->tableExists('hz_file_chunk'))
                    continue;

                if($settings['initialise'] !== true)
                    throw new \Exception($name . ' requires initialisation but initialise is disabled!');

                $schema = realpath(__DIR__ . str_repeat(DIRECTORY_SEPARATOR . '..', 2)
                    . DIRECTORY_SEPARATOR . 'libs'
                    . DIRECTORY_SEPARATOR . 'dbi_filesystem'
                    . DIRECTORY_SEPARATOR . 'schema.json');

                $manager = $fs_db->getSchemaManager();

                $this->log('Initialising DBI filesystem: ' . $name);

                if(!$manager->createSchemaFromFile($schema))
                    throw new \Exception('Unable to configure DBI filesystem schema!');

                //Look for the old tables and if they exists, do an upgrade!
                if($fs_db->tableExists('file') && $fs_db->tableExists('file_chunk')){

                    if(!$fs_db->hz_file_chunk->insert($fs_db->file_chunk->select('id', null, 'n', 'data')))
                        throw $fs_db->errorException();

                    if(!$fs_db->hz_file->insert($fs_db->file->find(array('kind' => 'dir'), array('id', 'kind', array('parent' => 'unnest(parents)'), null, 'filename', 'created_on', 'modified_on', 'length', 'mime_type', 'md5', 'owner', 'group', 'mode', 'metadata'))))
                        throw $fs_db->errorException();

                    $fs_db->driver->repair();

                    if(!$fs_db->query("INSERT INTO hz_file (kind, parent, start_chunk, filename, created_on, modified_on, length, mime_type, md5, owner, \"group\", mode, metadata) SELECT kind, unnest(parents) as parent, (SELECT fc.id FROM file_chunk fc WHERE fc.file_id=f.id), filename, created_on, modified_on, length, mime_type, md5, owner, \"group\", mode, metadata FROM file f WHERE kind = 'file'"))
                        throw $fs_db->errorException();

                }

            }
            catch(\Exception $e){

                $this->log($e->getMessage());

                continue;

            }

        }

    }

}
