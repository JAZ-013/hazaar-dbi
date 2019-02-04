<?php

/**
 * @file        Hazaar/DBI/DBD/BaseDriver.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2012 Jamie Carl (http://www.hazaarlabs.com)
 */

/**
 * @brief Relational Database Driver namespace
 */
namespace Hazaar\DBI\DBD;

/**
 * @brief Relational Database Driver Interface
 */
interface Driver_Interface {

    public function connect($dsn, $username = NULL, $password = NULL, $driver_options = NULL);

    public function beginTransaction();

    public function commit();

    public function rollBack();

    public function inTransaction();

    public function getAttribute($attribute);

    public function setAttribute($attribute, $value);

    public function lastInsertId();

    public function quote($string);

    public function exec($sql);

    /**
     *
     * @param
     *            $sql
     *
     * @return \Hazaar\DBI\Result
     */
    public function query($sql);

    public function prepare($sql);

    public function insert($table, $fields, $returning = 'id');

    public function update($table, $fields, $criteria = array());

    public function delete($table, $criteria);

    public function deleteAll($table);

}

/**
 * @brief Relational Database Driver - Base Class
 */
abstract class BaseDriver implements Driver_Interface {

    protected $allow_constraints = true;

    protected $reserved_words = array();

    protected $quote_special = '"';

    protected $pdo;

    protected $schema;

    protected static $execs = 0;

    /**
     * Master DBD connection
     *
     * A master connection can be created to perform write operations.  Reads will operate as normal
     * but any write operations such as INSERT, UPDATE, DELETE will be redirected to this connection
     *
     * @var BaseDriver
     */
    private $master;

    /**
     * SQL Commands to redirect to the master server connection
     *
     * @var mixed
     */
    static private $master_cmds = array('INSERT', 'UPDATE', 'DELETE');

    public function __construct($config = array()){

        $this->schema = ake($config, 'dbname', 'public');

    }

    public function setMasterDBD(BaseDriver $DBD){

        $this->master = $DBD;

    }

    public function __toString(){

        return strtoupper(basename(str_replace('\\', DIRECTORY_SEPARATOR, get_class($this))));

    }

    public function execCount() {

        return BaseDriver::$execs;

    }

    static function mkdsn($config){

        $options = $config->toArray();

        if(array_key_exists('driver', $options))
            unset($options['driver']);

        if(array_key_exists('master', $options))
            unset($options['master']);

        $dsn = $config->driver . ':' . array_flatten($options, '=', ';');

        return $dsn;

    }

    public function getSchemaName(){

        return $this->schema;

    }

    public function connect($dsn, $username = null, $password = null, $driver_options = null) {

        $this->pdo = new \PDO($dsn, $username, $password, $driver_options);

        return true;

    }

    public function beginTransaction() {

        return $this->pdo->beginTransaction();

    }

    public function commit() {

        return $this->pdo->commit();

    }

    public function getAttribute($attribute) {

        return $this->pdo->getAttribute($attribute);

    }

    public function inTransaction() {

        return $this->pdo->inTransaction();

    }

    public function lastInsertId() {

        return $this->pdo->lastInsertId();

    }

    public function quote($string) {

        if (is_string($string))
            $string = $this->pdo->quote($string);

        return $string;

    }

    public function quoteSpecial($string) {

        if (is_string($string)){

            $parts = explode('.', $string);

            array_walk($parts, function(&$item){
                $item = $this->quote_special . $item . $this->quote_special;
            });

            return implode('.', $parts);

        }

        return $string;

    }

    public function rollBack() {

        return $this->pdo->rollback();

    }

    public function setAttribute($attribute, $value) {

        return $this->pdo->setAttribute($attribute, $value);

    }

    public function errorCode() {

        return $this->pdo->errorCode();

    }

    public function errorInfo() {

        return $this->pdo->errorInfo();

    }

    private function getSQLType($sql){

        return strtoupper(substr($sql, 0, strpos($sql, ' ')));

    }

    public function exec($sql){

        if(!($this->master && in_array($this->getSQLType($sql), BaseDriver::$master_cmds, true)))
            return $this->pdo->exec($sql);

        return $this->master->exec($sql);

    }

    public function query($sql){

        if(!($this->master && in_array($this->getSQLType($sql), BaseDriver::$master_cmds, true)))
            return $this->pdo->query($sql);

        return $this->master->query($sql);

    }

    public function prepare($sql) {

        return $this->pdo->prepare($sql);

    }

    public function field($string) {

        if (in_array(strtoupper($string), $this->reserved_words))
            $string = $this->quoteSpecial($string);

        return $string;

    }

    public function type($string) {

        $types = array(
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamp',
            'character varying' => 'varchar'
        );

        if (array_key_exists($string, $types))
            return $types[$string];

        return $string;

    }

    public function prepareCriteria($criteria, $bind_type = 'AND', $tissue = '=', $parent_ref = NULL, $optional_key = NULL) {

        if(!is_array($criteria))
            return $criteria;

        $parts = array();

        foreach($criteria as $key => $value) {

            if(is_int($key) && is_string($value)){

                $parts[] = '( ' . $value . ' )';

            }elseif(substr($key, 0, 1) == '$') {

                $action = strtolower(substr($key, 1));

                switch($action) {
                    case 'and':

                        $parts[] = $this->prepareCriteria($value, 'AND');

                        break;

                    case 'or':
                        $parts[] = $this->prepareCriteria($value, 'OR');

                        break;

                    case 'ne':
                    case 'not' :

                        if(is_null($value))
                            $parts[] = 'IS NOT NULL';
                        else
                            $parts[] = '!= ' . $this->prepareValue($value);

                        break;

                    case  'ref' :
                        $parts[] = $tissue . ' ' . $value;

                        break;

                    case 'nin' :
                    case 'in' :

                        if(is_array($value) && count($value) > 0) {

                            $values = array();

                            foreach($value as $val)
                                $values[] = $this->prepareValue($val);

                            $parts[] = (($action == 'nin') ? 'NOT ' : NULL) . 'IN ( ' . implode(', ', $values) . ' )';

                        }

                        break;

                    case 'gt':

                        $parts[] = '> ' . $this->prepareValue($value);

                        break;

                    case 'gte':

                        $parts[] = '>= ' . $this->prepareValue($value);

                        break;

                    case 'lt':

                        $parts[] = '< ' . $this->prepareValue($value);

                        break;

                    case 'lte':

                        $parts[] = '<= ' . $this->prepareValue($value);

                        break;

                    case 'ilike': //iLike

                        $parts[] = 'ILIKE ' . $this->quote($value);

                        break;

                    case 'like': //Like

                        $parts[] = 'LIKE ' . $this->quote($value);

                        break;

                    case 'bt':

                        if(($count = count($value)) !== 2)
                            throw new \Exception('DBD: $bt operator requires array argument with exactly 2 elements. ' . $count . ' given.');

                        $parts[] = 'BETWEEN ' . $this->prepareValue(array_values($value)[0])
                            . ' AND ' . $this->prepareValue(array_values($value)[1]);

                        break;

                    case '~':
                    case '~*':
                    case '!~':
                    case '!~*':

                        $parts[] = $action . ' ' . $this->quote($value);

                        break;

                    case 'exists': //exists

                        foreach($value as $table => $criteria)
                            $parts[] = 'EXISTS ( SELECT * FROM ' . $table . ' WHERE ' . $this->prepareCriteria($criteria) . ' )';

                        break;

                    case 'sub': //sub query

                        $parts[] = '( ' . $value[0]->toString(FALSE) . ' ) ' . $this->prepareCriteria($value[1]);

                        break;

                    case 'array':

                        if(!is_array($value))
                            $value = array($value);

                        foreach($value as &$val)
                            $val = $this->prepareValue($val);

                        $parts[] = 'ARRAY[' . implode(',', $value) . ']';

                        break;

                    case 'push':

                        if(!is_array($value))
                            $value = array($value);

                        foreach($value as &$val)
                            $val = $this->prepareValue($val);

                        $parts[] = $this->field($optional_key) . ' || ARRAY[' . implode(',', $value) . ']';

                        break;

                    default :
                        $parts[] = ' ' . $tissue . ' ' . $this->prepareCriteria($value, strtoupper(substr($key, 1)));

                        break;
                }

            } else {

                if(is_array($value)) {

                    $sub_value = $this->prepareCriteria($value);

                    if(is_numeric($key)) {

                        $parts[] = $sub_value;

                    }else{

                        if($parent_ref && strpos($key, '.') === FALSE)
                            $key = $parent_ref . '.' . $key;

                        $parts[] = $key . ' ' . $sub_value;

                    }

                } else {

                    if($parent_ref && strpos($key, '.') === FALSE)
                        $key = $parent_ref . '.' . $key;

                    if(is_null($value))
                        $joiner = 'IS ' . (($tissue == '!=') ? 'NOT ' : NULL);
                    else
                        $joiner = $tissue;

                    $parts[] = $this->field($key) . ' ' . $joiner . ' ' . $this->prepareValue($value);

                }

            }

        }

        $sql = '';

        if(count($parts) > 0)
            $sql = ((count($parts) > 1) ? '( ' : NULL) . implode(" $bind_type ", $parts) . ((count($parts) > 1) ? ' )' : NULL);

        return $sql;

    }

    public function prepareFields($fields) {

        $field_def = array();

        foreach($fields as $key => $value) {

            if (is_numeric($key))
                $field_def[] = $this->field($value);
            else
                $field_def[] = $this->field($value) . ' AS ' . $this->field($key);

        }

        return implode(', ', $field_def);

    }

    public function prepareValue($value, $key = null) {

        if (is_array($value)) {

            $value = $this->prepareCriteria($value, NULL, NULL, NULL, $key);

        } elseif ($value instanceof \Hazaar\Date) {

            $value = $this->quote($value->format('Y-m-d H:i:s'));

        } else if (is_null($value)) {

            $value = 'NULL';

        } else if (is_bool($value)) {

            $value = ($value ? 'TRUE' : 'FALSE');

        } else if ($value instanceof \stdClass) {

            $value = $this->quote(json_encode($value));

        } else if (!is_int($value) && $value[0] != ':') {

            $value = $this->quote((string) $value);

        }

        return $value;

    }

    /**
     * Special internal function to fix the default column value.
     *
     * This function is normally overridden by the DBD class being used so that values can be "fixed".
     *
     * @param mixed $value
     *
     * @return mixed
     */
    protected function fixValue($value){

        return $value;

    }

    public function insert($table, $fields, $returning = NULL) {

        if($fields instanceof \Hazaar\Map)
            $fields = $fields->toArray();
        elseif($fields instanceof \Hazaar\Model\Strict)
            $fields = $fields->toArray(false, null, false);
        elseif($fields instanceof \stdClass)
            $fields = (array)$fields;

        $field_def = array_keys($fields);

        foreach($field_def as &$field)
            $field = $this->field($field);

        $value_def = array_values($fields);

        foreach($value_def as $key => &$value)
            $value = $this->prepareValue($value, $field_def[$key]);

        $sql = 'INSERT INTO ' . $this->field($table) . ' ( ' . implode(', ', $field_def) . ' ) VALUES ( ' . implode(', ', $value_def) . ' )';

        $return_value = FALSE;

        if ($returning === NULL || $returning === FALSE) {

            $sql .= ';';

            $return_value = $this->exec($sql);

        } elseif ($returning === TRUE) {

            $sql .= ';';

            if ($result = $this->query($sql))
                $return_value = (int) $this->lastinsertid();

        } elseif (is_string($returning)) {

            $sql .= ' RETURNING ' . $returning . ';';

            if ($result = $this->query($sql))
                $return_value = $result->fetchColumn(0);

        }

        return $return_value;

    }

    public function update($table, $fields, $criteria = array()) {

        if($fields instanceof \Hazaar\Map)
            $fields = $fields->toArray();
        elseif($fields instanceof \Hazaar\Model\Strict)
            $fields = $fields->toArray(false, null, false);
        elseif($fields instanceof \stdClass)
            $fields = (array)$fields;

        $field_def = array();

        foreach($fields as $key => $value)
            $field_def[] = $this->field($key) . ' = ' . $this->prepareValue($value, $key);

        if (count($field_def) == 0)
            throw new Exception\NoUpdate();

        $sql = 'UPDATE ' . $this->field($table) . ' SET ' . implode(', ', $field_def);

        if (count($criteria) > 0)
            $sql .= ' WHERE ' . $this->prepareCriteria($criteria);

        $sql .= ';';

        return $this->exec($sql);

    }

    public function delete($table, $criteria) {

        $sql = 'DELETE FROM ' . $this->field($table) . ' WHERE ' . $this->prepareCriteria($criteria) . ';';

        return $this->exec($sql);

    }

    public function deleteAll($table) {

        $sql = 'DELETE FROM ' . $this->field($table) . ';';

        return $this->exec($sql);

    }

    /*
     * Database information methods
     */
    public function listTables() {

        return array();

    }

    public function tableExists($table) {

        $stmt = $this->query('SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='
            . $this->quote($table) . ' AND table_schema='
            . $this->quote($this->schema) . ');');

        return $stmt->fetchColumn(0);

    }

    public function createTable($name, $columns) {

        $sql = "CREATE TABLE " . $this->field($name) . " (\n";

        $coldefs = array();

        $constraints = array();

        foreach($columns as $name => $info) {

            if (is_array($info)) {

                if (is_numeric($name)) {

                    if (!array_key_exists('name', $info))
                        throw new \Exception('Error creating new table.  Name is a number which is not allowed!');

                    $name = $info['name'];

                }

                $def = $this->field($name) . ' ' . $this->type($info['data_type']) . (ake($info, 'length') ? '(' . $info['length'] . ')' : NULL);

                if (array_key_exists('default', $info) && $info['default'] !== NULL)
                    $def .= ' DEFAULT ' . $info['default'];

                if (array_key_exists('not_null', $info) && $info['not_null'])
                    $def .= ' NOT NULL';

                if (array_key_exists('primarykey', $info) && $info['primarykey']) {

                    $driver = strtolower(basename(str_replace('\\', '/', get_class($this))));

                    if ($driver == 'pgsql')
                        $constraints[] = ' PRIMARY KEY(' . $this->field($name) . ')';

                    else
                        $def .= ' PRIMARY KEY';

                }

            } else {

                $def = "\t" . $this->field($name) . ' ' . $info;

            }

            $coldefs[] = $def;

        }

        $sql .= implode(",\n", $coldefs);

        if (count($constraints) > 0)
            $sql .= ",\n" . implode(",\n", $constraints);

        $sql .= "\n);";

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            throw new \Exception('Could not create table. ' . $this->errorInfo()[2]);

        return TRUE;

    }

    public function describeTable($name, $sort = NULL) {

        if (!$sort)
            $sort = 'ordinal_position';

        $result = $this->query('SELECT * FROM information_schema.columns WHERE table_name='
            . $this->quote($name) . ' AND table_schema='
            . $this->quote($this->schema) . ' ORDER BY '
            . $sort);

        $columns = array();

        while($col = $result->fetch(\PDO::FETCH_ASSOC)) {

            $col = array_change_key_case($col, CASE_LOWER);

            if (preg_match('/nextval\(\'(\w*)\'::regclass\)/', $col['column_default'], $matches)) {

                if ($info = $this->describeSequence($matches[1])) {

                    $col['data_type'] = 'serial';

                    $col['column_default'] = NULL;

                }

            }

            //Fixed array types to their actual SQL array data type
            if($col['data_type'] == 'ARRAY'
                && ($udt_name = ake($col, 'udt_name'))){

                if($udt_name[0] == '_')
                    $col['data_type'] = substr($udt_name, 1) . '[]';

            }

            $columns[] = array(
                'name' => $col['column_name'],
                'ordinal_position' => $col['ordinal_position'],
                'default' => $this->fixValue($col['column_default']),
                'not_null' => (($col['is_nullable'] == 'NO') ? TRUE : FALSE),
                'data_type' => $this->type($col['data_type']),
                'length' => $col['character_maximum_length']
            );

        }

        return $columns;

    }

    public function renameTable($from_name, $to_name) {

        if (strpos($to_name, '.')) {

            list($from_schema, $from_name) = explode('.', $from_name);

            list($to_schema, $to_name) = explode('.', $to_name);

            if ($to_schema != $from_schema)
                throw new \Exception('You can not rename tables between schemas!');

        }

        $sql = "ALTER TABLE " . $this->field($from_name) . " RENAME TO " . $this->field($to_name) . ";";

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function dropTable($name, $cascade = false) {

        $sql = "DROP TABLE " . $this->field($name) . ($cascade?' CASCADE':'') . ";";

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function addColumn($table, $column_spec) {

        if (!array_key_exists('name', $column_spec))
            return FALSE;

        if (!array_key_exists('data_type', $column_spec))
            return FALSE;

        $sql = 'ALTER TABLE ' . $this->field($table) . ' ADD COLUMN ' . $this->field($column_spec['name']) . ' ' . $this->type($column_spec['data_type']);

        if (array_key_exists('not_null', $column_spec) && $column_spec['not_null'])
            $sql .= ' NOT NULL';

        if (array_key_exists('default', $column_spec) && $column_spec['default'] !== null)
            $sql .= ' DEFAULT ' . $column_spec['default'];

        $sql .= ';';

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function alterColumn($table, $column, $column_spec) {

        $sqls = array();

        //Check if the column is being renamed and update the name first.
        if(array_key_exists('name', $column_spec)){

            $sql = "ALTER TABLE " . $this->field($table) . " RENAME COLUMN " . $this->field($column) . ' TO ' . $this->field($column_spec['name']);

            $this->exec($sql);

            $column = $column_spec['name'];

        }

        $prefix = "ALTER TABLE " . $this->field($table) . " ALTER COLUMN " . $this->field($column);

        if (array_key_exists('data_type', $column_spec)){

            $alter_type = $prefix . " TYPE " . $this->type($column_spec['data_type']) . ((array_key_exists('length', $column_spec) && $column_spec['length'] > 0) ? '(' . $column_spec['length'] . ')' : NULL);

            if (array_key_exists('using', $column_spec))
                $alter_type .= ' USING ' . $column_spec['using'];

            $sqls[] = $alter_type;

        }

        if (array_key_exists('not_null', $column_spec))
            $sqls[] = $prefix . ' ' . ($column_spec['not_null'] ? 'SET' : 'DROP') . ' NOT NULL';

        if (array_key_exists('default', $column_spec))
            $sqls[] .= $prefix . ' ' . ($column_spec['default'] === null ? 'DROP DEFAULT' : 'SET DEFAULT ' . $column_spec['default'] );

        foreach($sqls as $sql)
            $this->exec($sql);

        return TRUE;

    }

    public function dropColumn($table, $column) {

        $sql = "ALTER TABLE " . $this->field($table) . " DROP COLUMN " . $this->field($column) . ";";

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function listSequences() {

        $result = $this->query("SELECT sequence_schema as schema, sequence_name as name
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ( 'information_schema', 'pg_catalog');");

        return $result->fetchAll(\PDO::FETCH_ASSOC);

    }

    public function describeSequence($name) {

        $sql = "SELECT * FROM information_schema.sequences WHERE sequence_name = '$name'";

        if ($this->schema)
            $sql .= " AND sequence_schema = '$this->schema'";

        $sql .= ';';

        $result = $this->query($sql);

        return $result->fetchAll(\PDO::FETCH_ASSOC);

    }

    public function listIndexes($table = NULL){

        return array();

    }

    public function createIndex($index_name, $table_name, $idx_info) {

        if (!array_key_exists('columns', $idx_info))
            return false;

        $indexes = $this->listIndexes($table_name);

        if(array_key_exists($index_name, $indexes))
            return true;

        $sql = 'CREATE';

        if (array_key_exists('unique', $idx_info) && $idx_info['unique'])
            $sql .= ' UNIQUE';

        $sql .= " INDEX " . $this->field($index_name) . " ON " . $this->field($table_name) . " (" . implode(',', array_map(array($this, 'field'), $idx_info['columns'])) . ')';

        if (array_key_exists('using', $idx_info) && $idx_info['using'])
            $sql .= ' USING ' . $idx_info['using'];

        $sql .= ';';

        $affected = $this->exec($sql);

        if ($affected === false)
            return false;

        return true;

    }

    public function dropIndex($name) {

        $sql = $this->exec('DROP INDEX ' . $this->field($name));

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function listConstraints($table = NULL, $type = NULL, $invert_type = FALSE) {

        return array();

    }

    public function addConstraint($name, $info) {

        if(!$this->allow_constraints)
            return false;

        if(!array_key_exists('table', $info))
            return false;

        if (!array_key_exists('type', $info) || !$info['type']){

            if(array_key_exists('references', $info))
                $info['type'] = 'FOREIGN KEY';
            else
                return FALSE;

        }

        if($info['type'] == 'FOREIGN KEY'){

            if(!array_key_exists('update_rule', $info))
                $info['update_rule'] = 'NO ACTION';

            if(!array_key_exists('delete_rule', $info))
                $info['delete_rule'] = 'NO ACTION';

        }

        $column = $info['column'];

        if(is_array($column)){

            foreach($column as &$col)
                $col = $this->field($col);

            $column = implode(', ', $column);

        }else{

            $column = $this->field($column);

        }

        $sql = "ALTER TABLE " . $this->field($info['table']) . " ADD CONSTRAINT " . $this->field($name) . " $info[type] (" . $column . ")";

        if (array_key_exists('references', $info))
            $sql .= " REFERENCES " . $this->field($info['references']['table']) . " (" . $this->field($info['references']['column']) . ") ON UPDATE $info[update_rule] ON DELETE $info[delete_rule]";

        $sql .= ';';

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function dropConstraint($name, $table, $cascade = false) {

        $sql = "ALTER TABLE " . $this->field($table) . " DROP CONSTRAINT " . $this->field($name) . ($cascade?' CASCADE':'') . ';';

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function listViews(){

        return false;

    }

    public function describeView($name){

        return false;

    }

    public function createView($name, $content){

        $sql = 'CREATE OR REPLACE VIEW ' . $this->field($name) . ' AS ' . rtrim($content, ' ;') . ';';

        return ($this->exec($sql) !== false);

    }

    public function dropView($name, $cascade = false){

        $sql = 'DROP VIEW ' . $this->field($name);

        if($cascade === true)
            $sql .= ' CASCADE';

        $sql .= ';';

        return ($this->exec($sql) !== false);

    }

    public function listFunctions(){

        $sql = "SELECT r.routine_schema, r.routine_name FROM INFORMATION_SCHEMA.routines r WHERE r.specific_schema='public';";

        $q = $this->query($sql);

        $list = array();

        while($row = $q->fetch()){

            $id = $row['routine_schema'] . $row['routine_name'];

            if(array_key_exists($id, $list))
                continue;

            $list[$id] = array('schema' => $row['routine_schema'], 'name' => $row['routine_name']);

        }

        return array_values($list);

    }

    public function describeFunction($name, $schema = null){

        $sql = "SELECT r.specific_name, r.routine_schema, r.routine_name, r.data_type, r.routine_body, r.routine_definition,
            p.parameter_name, p.data_type, p.parameter_mode, p.ordinal_position
            FROM INFORMATION_SCHEMA.routines r
            INNER JOIN INFORMATION_SCHEMA.parameters p ON p.specific_name=r.specific_name";

        $sql .= " WHERE r.specific_schema=" . $this->prepareValue($this->schema);

        $sql .= " AND r.routine_name=" . $this->prepareValue($name) ." ORDER BY r.routine_name, p.ordinal_position;";

        if(!($q = $this->query($sql)))
            throw new \Exception($this->errorInfo()[2]);

        $info = array();

        while($row = $q->fetch()){

            if(!array_key_exists($row['specific_name'], $info)){

                $item = array(
                    'schema' => $row['routine_schema'],
                    'name' => $row['routine_name'],
                    'return_type' => $row['data_type'],
                    'lang' => $row['routine_body'],
                    'content' => trim($row['routine_definition'])
                );

                $item['parameters'] = array();

                $info[$row['specific_name']] = $item;

            }

            $info[$row['specific_name']]['parameters'][] = array(
                'name' => $row['parameter_name'],
                'type' => $row['data_type'],
                'mode' => $row['parameter_mode'],
                'ordinal_position' => $row['ordinal_position']
            );

        }

        usort($info, function($a, $b){
            if(count($a['parameters']) === count($b['parameters'])) return 0;
            return count($a['parameters']) < count($b['parameters']) ? -1 : 1;
        });

        return array_values($info);

    }

    public function createFunction($name, $spec){

        $sql = 'CREATE OR REPLACE FUNCTION ' . $this->field($name) . ' (';

        if($params = ake($spec, 'parameters')){

            $items = array();

            foreach($params as $param)
                $items[] = ake($param, 'mode', 'IN') . ' ' . ake($param, 'name') . ' ' . ake($param, 'type');

            $sql .= implode(', ', $items);

        }

        $sql .= ') RETURNS ' . ake($spec, 'return_type', 'TEXT') . ' LANGUAGE ' . ake($spec, 'lang', 'SQL') . " AS\n\$BODY$ ";

        $sql .= ake($spec, 'content');

        $sql .= '$BODY$;';

        return ($this->exec($sql) !== false);

    }

    public function dropFunction($name, $arg_types = array(), $cascade = false){

        $sql = 'DROP FUNCTION IF EXISTS ' . $this->field($name);

        if($arg_types)
            $sql .= ' (' . (is_array($arg_types) ? implode(', ', $arg_types) : $arg_types) . ')';

        if($cascade === true)
            $sql .= ' CASCADE';

        $sql .= ';';

        return ($this->exec($sql) !== false);

    }

}

