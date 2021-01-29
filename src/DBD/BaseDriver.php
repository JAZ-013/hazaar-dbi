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

    public function setTimezone($tz);

    public function repair();

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

    public function insert($table, $fields, $returning = null);

    public function update($table, $fields, $criteria = array());

    public function delete($table, $criteria);

    public function deleteAll($table);

}

/**
 * @brief Relational Database Driver - Base Class
 */
abstract class BaseDriver implements Driver_Interface {

    static public $dsn_elements = array();

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
    protected $master;

    /**
     * SQL Commands to redirect to the master server connection
     *
     * @var mixed
     */
    static private $master_cmds = array('INSERT', 'UPDATE', 'DELETE');

    static public $select_groups = array();

    public function __construct($config = array()){

        $this->schema = ake($config, 'dbname', 'public');

    }

    public function setMasterDBD(BaseDriver $DBD){

        $this->master = $DBD;

    }

    public function setTimezone($tz){

        return false;

    }

    public function __toString(){

        return strtoupper(basename(str_replace('\\', DIRECTORY_SEPARATOR, get_class($this))));

    }

    public function execCount() {

        return BaseDriver::$execs;

    }

    static function mkdsn($config){

        $options = $config->toArray();

        $DBD = 'Hazaar\\DBI\\DBD\\' . ucfirst($config->driver);

        if(!class_exists($DBD))
            return false;

        $options = array_intersect_key($options, array_combine($DBD::$dsn_elements, $DBD::$dsn_elements));

        return $config->driver . ':' . array_flatten($options, '=', ';');

    }

    public function getSchemaName(){

        return $this->schema;

    }

    public function setSchemaName($schema){

        $this->schema = $schema;

    }

    public function connect($dsn, $username = null, $password = null, $driver_options = null) {

        try{

            $this->pdo = new \PDO($dsn, $username, $password, $driver_options);

        }
        catch(\Exception $e){

            return false;

        }

        return true;

    }

    public function repair(){

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

        $sql = rtrim($sql, '; ') . ';';

        if(!($this->master && in_array($this->getSQLType($sql), BaseDriver::$master_cmds, true)))
            return $this->pdo->exec($sql);

        return $this->master->exec($sql);

    }

    public function query($sql){

        $sql = rtrim($sql, '; ') . ';';

        if(!($this->master && in_array($this->getSQLType($sql), BaseDriver::$master_cmds, true)))
            return $this->pdo->query($sql);

        return $this->master->query($sql);

    }

    public function prepare($sql) {

        return $this->pdo->prepare($sql);

    }

    public function schemaTable($table){

        $alias = null;

        //Check if there is an alias
        if(($pos = strpos($table, ' ')) !== false)
            list($table, $alias) = preg_split('/\s*(?<=.{'.$pos.'})\s*/', $table, 2);

        //Check if we already have a schema defined
        if(strpos($table, '.') === false)
            $table = $this->schema . '.' . $table;

        return $this->quoteSpecial($table) . ($alias ? ' ' . $this->quoteSpecial($alias) : '');
    }

    public function field($string) {

        if (in_array(strtoupper($string), $this->reserved_words))
            $string = $this->quoteSpecial($string);

        return $string;

    }

    public function type($info) {

        if(!($type = ake($info, 'data_type')))
            return false;

        $array = false;

        $types = array(
            'timestamp without time zone' => 'timestamp',
            'timestamp with time zone' => 'timestamp',
            'character varying' => 'varchar'
        );

        if($array = (substr($type, -2) === '[]'))
            $type = substr($type, 0, -2);

        if (array_key_exists($type, $types))
            $type = $types[$type];

        return $type . (ake($info, 'length') ? '(' . $info['length'] . ')' : NULL) . ($array ? '[]' : '');

    }

    public function prepareCriteria($criteria, $bind_type = 'AND', $tissue = '=', $parent_ref = NULL, $optional_key = NULL, &$set_key = true) {

        if(!is_array($criteria))
            return $criteria;

        $parts = array();

        foreach($criteria as $key => $value) {

            if(is_int($key) && is_string($value)){

                $parts[] = '( ' . $value . ' )';

            }elseif(substr($key, 0, 1) == '$') {

                if($action_parts = $this->prepareCriteriaAction(strtolower(substr($key, 1)), $value, $tissue, $optional_key, $set_key)){

                    if(is_array($action_parts))
                        $parts = array_merge($parts, $action_parts);
                    else
                        $parts[] = $action_parts;

                } else $parts[] = ' ' . $tissue . ' ' . $this->prepareCriteria($value, strtoupper(substr($key, 1)));

            } else {

                if(is_array($value)) {

                    $set = true;

                    $sub_value = $this->prepareCriteria($value, $bind_type, $tissue, $parent_ref, $key, $set);

                    if(is_numeric($key)) {

                        $parts[] = $sub_value;

                    }else{

                        if($parent_ref && strpos($key, '.') === FALSE)
                            $key = $parent_ref . '.' . $key;

                        $parts[] = (($set === true) ? $key . ' ' : '' ) . $sub_value;

                    }

                } else {

                    if($parent_ref && strpos($key, '.') === FALSE)
                        $key = $parent_ref . '.' . $key;

                    if(is_null($value) || is_boolean($value))
                        $joiner = 'IS' . (($tissue === '!=') ? 'NOT' : NULL);
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

    public function prepareCriteriaAction($action, $value, $tissue = '=', $key = null, &$set_key = true){

        switch($action) {
            case 'and':

                return $this->prepareCriteria($value, 'AND');

            case 'or':

                return $this->prepareCriteria($value, 'OR');

            case 'ne':

                if(is_null($value))
                    return 'IS NOT NULL';
                
                return (is_boolean($value) ? 'IS NOT ' : '!= ') . $this->prepareValue($value);

            case 'not' :

                return 'NOT (' . $this->prepareCriteria($value) . ')';

            case  'ref' :

                return $tissue . ' ' . $value;

            case 'nin' :
            case 'in' :

                if(is_array($value) && count($value) > 0) {

                    $values = array();

                    foreach($value as $val)
                        $values[] = $this->prepareValue($val);

                    return (($action == 'nin') ? 'NOT ' : NULL) . 'IN ( ' . implode(', ', $values) . ' )';

                }

                break;

            case 'gt':

                return '> ' . $this->prepareValue($value);

            case 'gte':

                return '>= ' . $this->prepareValue($value);

            case 'lt':

                return '< ' . $this->prepareValue($value);

            case 'lte':

                return '<= ' . $this->prepareValue($value);

            case 'ilike': //iLike

                return 'ILIKE ' . $this->quote($value);

            case 'like': //Like

                return 'LIKE ' . $this->quote($value);

            case 'bt':

                if(($count = count($value)) !== 2)
                    throw new \Hazaar\Exception('DBD: $bt operator requires array argument with exactly 2 elements. ' . $count . ' given.');

                return 'BETWEEN ' . $this->prepareValue(array_values($value)[0])
                    . ' AND ' . $this->prepareValue(array_values($value)[1]);

            case '~':
            case '~*':
            case '!~':
            case '!~*':

                return $action . ' ' . $this->quote($value);

            case 'exists': //exists

                $parts = array();

                foreach($value as $table => $criteria)
                    $parts[] = 'EXISTS ( SELECT * FROM ' . $this->schemaTable($table) . ' WHERE ' . $this->prepareCriteria($criteria) . ' )';

                return $parts;

            case 'sub': //sub query

                return '( ' . $value[0]->toString(FALSE) . ' ) ' . $this->prepareCriteria($value[1]);

            case 'json':

                return $this->prepareValue(json_encode($value, JSON_UNESCAPED_UNICODE));

        }

        return null;
        
    }

    public function prepareFields($fields, $exclude = array(), $tables = array()) {

        if(!is_array($fields))
            return $this->field($fields);

        if(!is_array($exclude))
            $exclude = array();

        $field_def = array();

        foreach($fields as $key => $value) {

            if($value instanceof \Hazaar\DBI\Table)
                $value = (($value->limit() === 1) ? '(' : 'array(') . $value . ')';

            if(is_string($value) && in_array($value, $exclude))
                $field_def[] = $value;
            elseif (is_numeric($key))
                $field_def[] = is_array($value) ? $this->prepareFields($value, null, $tables) : $this->field($value);
            elseif(is_array($value)){

                $fields = array();

                $field_map = array_to_dot_notation(array($key => $this->prepareArrayAliases($value)));

                foreach($field_map as $alias => $field){

                    $lookup = md5(uniqid('dbi_', true));

                    self::$select_groups[$lookup] = $alias;

                    $fields[$lookup] = $field;

                }

                $field_def[] = $this->prepareFields($fields, null, $tables);

            }elseif(preg_match('/^((\w+)\.)?\*$/', trim($value), $matches) > 0){

                if(count($matches) > 1)
                    $alias = ake($tables, $matches[2]);
                else{

                    $alias = reset($tables);

                    $value = key($tables) . '.*';

                }

                self::$select_groups[$alias] = $key;

                $field_def[] = $this->field($value);

            }else
                $field_def[] = $this->field($value) . ' AS ' . $this->field($key);

        }

        return implode(', ', $field_def);

    }

    private function prepareArrayAliases($array){

        if(!is_array($array))
            return $array;

        foreach($array as $key => &$value){

            if(is_array($value))
                $value = $this->prepareArrayAliases($value);
            elseif(is_string($value) && substr($value, -1) === '*')
                continue;

            if(!is_numeric($key))
                continue;

            unset($array[$key]);

            $key = $value;

            if(($pos = strrpos($key, '.')) > 0)
                $key = substr($key, $pos + 1);

            $array[$key] = $value;

        }

        return $array;

    }

    public function prepareValue($value, $key = null) {

        if (is_array($value) && count($value) > 0) {

            $value = $this->prepareCriteria($value, NULL, NULL, NULL, $key);

        } elseif ($value instanceof \Hazaar\Date) {

            $value = $this->quote($value->format('Y-m-d H:i:s'));

        } else if (is_null($value) || (is_array($value) && count($value) === 0)) {

            $value = 'NULL';

        } else if (is_bool($value)) {

            $value = ($value ? 'TRUE' : 'FALSE');

        } else if ($value instanceof \stdClass) {

            $value = $this->quote(json_encode($value));

        } else if (!is_int($value) && (substr($value, 0, 1) !== ':' || substr($value, 1, 1) === ':')){

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

    public function insert($table, $fields, $returning = null, $conflict_target = null, $conflict_update = null) {

        if($fields instanceof \Hazaar\Map)
            $fields = $fields->toArray();
        elseif($fields instanceof \Hazaar\Model\Strict)
            $fields = $fields->toArray(false, null, array('insert' => false, 'dbi' => false, 'hide' => true));
        elseif($fields instanceof \stdClass)
            $fields = (array)$fields;

        $sql = 'INSERT INTO ' . $this->schemaTable($table);

        if($fields instanceof \Hazaar\DBI\Table){

            $sql .= ' ' . (string)$fields;

        }else{

            $field_def = array_keys($fields);

            foreach($field_def as &$field)
                $field = $this->field($field);

            $value_def = array_values($fields);

            foreach($value_def as $key => &$value)
                $value = $this->prepareValue($value, null, $field_def[$key]);

            $sql .= ' ( ' . implode(', ', $field_def) . ' ) VALUES ( ' . implode(', ', $value_def) . ' )';

        }

        if($conflict_target !== null){

            $sql .= ' ON CONFLICT(' . $this->field($conflict_target) . ')';

            if($conflict_update === null){

                $sql .= ' DO NOTHING';

            }else{
                
                if($conflict_update === true)
                    $conflict_update = array_keys($fields);

                if(is_array($conflict_update) && count($conflict_update) > 0){

                    $update_defs = array();

                    foreach($conflict_update as $field){

                        if(!array_key_exists($field, $fields))
                            continue;

                        $update_defs[] = $this->field($field) . ' = EXCLUDED.' . $field;

                    }

                    $sql .= ' DO UPDATE SET ' . implode(', ', $update_defs);

                }

            }

        }

        $return_value = FALSE;

        if ($returning === NULL || $returning === FALSE || (is_array($returning) && count($returning) === 0)) {

            $return_value = $this->exec($sql);

        } elseif ($returning === TRUE) {

            if ($result = $this->query($sql))
                $return_value = (int) $this->lastinsertid();

        } else{
            
            if (is_string($returning)){

                $returning = trim($returning);
    
                $sql .= ' RETURNING ' . $this->field($returning);
    
            }elseif(is_array($returning) && count($returning) > 0)
                $sql .= ' RETURNING ' . $this->prepareFields($returning);
            
            if ($result = $this->query($sql))
                $return_value = (is_string($returning) && $returning !== '*') ? $result->fetchColumn(0) : $result;

        }
        
        return $return_value;

    }

    public function update($table, $fields, $criteria = array(), $from = array(), $returning = null) {

        if($fields instanceof \Hazaar\Map)
            $fields = $fields->toArray();
        elseif($fields instanceof \Hazaar\Model\Strict){

            $data = $fields->toArray(false, null, array('update' => false, 'dbi' => false, 'hide' => true));

            //Convert any arrays into JSON defs if needed
            foreach($data as $key => &$value){

                if(!is_array($value)) continue;

                $def = $fields->getDefinition($key);

                $type = ake($def, 'type');

                if($type === 'model' || ($type === 'array' && array_key_exists('items', $def)))
                    $value = array('$json' => $value);
                elseif($type === 'array')
                    $value = array('$array' => $value);

            }

            $fields = $data;

        }elseif($fields instanceof \stdClass)
            $fields = (array)$fields;

        $field_def = array();

        foreach($fields as $key => &$value)
            $field_def[] = $this->field($key) . ' = ' . $this->prepareValue($value, $key);

        if (count($field_def) == 0)
            throw new Exception\NoUpdate();

        $table = (is_array($table) && isset($table[0])) ? $this->schemaTable($table[0]) . ' AS ' . $table[1] : $this->schemaTable($table);

        $sql = 'UPDATE ' . $table . ' SET ' . implode(', ', $field_def);

        if(is_array($from) && count($from) > 0)
            $sql .= ' FROM ' . implode(', ', $from);

        if(is_array($criteria) && count($criteria) > 0)
            $sql .= ' WHERE ' . $this->prepareCriteria($criteria);

        $return_value = FALSE;

        if ($returning === TRUE)
            $returning = '*';

        if ($returning === NULL || $returning === FALSE || (is_array($returning) && count($returning) === 0)) {

            $return_value = $this->exec($sql);

        } else{
            
            if (is_string($returning)){

                $returning = trim($returning);
    
                $sql .= ' RETURNING ' . $this->field($returning);
    
            }elseif(is_array($returning) && count($returning) > 0)
                $sql .= ' RETURNING ' . $this->prepareFields($returning);
            
            if ($result = $this->query($sql))
                $return_value = (is_string($returning) && $returning !== '*') ? $result->fetchColumn(0) : $result;

        }

        return $return_value;

    }

    public function delete($table, $criteria, $from = array()) {

        $sql = 'DELETE FROM ' . $this->schemaTable($table);

        if(is_array($from) && count($from) > 0)
            $sql .= ' USING ' . implode(', ', $from);

        $sql .= ' WHERE ' . $this->prepareCriteria($criteria);

        return $this->exec($sql);

    }

    public function deleteAll($table) {

        return $this->exec('DELETE FROM ' . $this->schemaTable($table));

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

    public function createTable($table_name, $columns) {

        $sql = "CREATE TABLE " . $this->schemaTable($table_name) . " (\n";

        $coldefs = array();

        $constraints = array();

        if($table_name === 'oauth2_auth')
            echo '';

        foreach($columns as $name => $info) {

            if (is_array($info)) {

                if (is_numeric($name)) {

                    if (!array_key_exists('name', $info))
                        throw new \Hazaar\Exception('Error creating new table.  Name is a number which is not allowed!');

                    $name = $info['name'];

                }
                
                if(!($type = $this->type($info)))
                    throw new \Hazaar\Exception("Column '$name' has no data type!");
                
                $def = $this->field($name) . ' ' . $type;

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
            throw new \Hazaar\Exception("Could not create table '$table_name'. " . $this->errorInfo()[2]);

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
                'default' => $this->fixValue($col['column_default']),
                'not_null' => (($col['is_nullable'] == 'NO') ? TRUE : FALSE),
                'data_type' => $this->type($col),
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
                throw new \Hazaar\Exception('You can not rename tables between schemas!');

        }

        $sql = "ALTER TABLE " . $this->schemaTable($from_name) . " RENAME TO " . $this->field($to_name) . ";";

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function dropTable($name, $cascade = false) {

        $sql = "DROP TABLE " . $this->schemaTable($name) . ($cascade?' CASCADE':'') . ";";

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

        $sql = 'ALTER TABLE ' . $this->schemaTable($table) . ' ADD COLUMN ' . $this->field($column_spec['name']) . ' ' . $this->type($column_spec);

        if (array_key_exists('not_null', $column_spec) && $column_spec['not_null'])
            $sql .= ' NOT NULL';

        if (array_key_exists('default', $column_spec) && $column_spec['default'] !== null)
            $sql .= ' DEFAULT ' . $column_spec['default'];

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function alterColumn($table, $column, $column_spec) {

        $sqls = array();

        //Check if the column is being renamed and update the name first.
        if(array_key_exists('name', $column_spec)){

            $sql = "ALTER TABLE " . $this->schemaTable($table) . " RENAME COLUMN " . $this->field($column) . ' TO ' . $this->field($column_spec['name']);

            $this->exec($sql);

            $column = $column_spec['name'];

        }

        $prefix = "ALTER TABLE " . $this->schemaTable($table) . " ALTER COLUMN " . $this->field($column);

        if (array_key_exists('data_type', $column_spec)){

            $alter_type = $prefix . " TYPE " . $this->type($column_spec);

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

        $sql = "ALTER TABLE " . $this->schemaTable($table) . " DROP COLUMN " . $this->field($column) . ";";

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

        $sql .= " INDEX " . $this->field($index_name) . " ON " . $this->schemaTable($table_name) . " (" . implode(',', array_map(array($this, 'field'), $idx_info['columns'])) . ')';

        if (array_key_exists('using', $idx_info) && $idx_info['using'])
            $sql .= ' USING ' . $idx_info['using'];

        $affected = $this->exec($sql);

        if ($affected === false)
            return false;

        return true;

    }

    public function dropIndex($name) {

        $sql = 'DROP INDEX ' . $this->field($name);

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

        $sql = "ALTER TABLE " . $this->schemaTable($info['table']) . " ADD CONSTRAINT " . $this->field($name) . " $info[type] (" . $column . ")";

        if (array_key_exists('references', $info))
            $sql .= " REFERENCES " . $this->schemaTable($info['references']['table']) . " (" . $this->field($info['references']['column']) . ") ON UPDATE $info[update_rule] ON DELETE $info[delete_rule]";

        $affected = $this->exec($sql);

        if ($affected === FALSE)
            return FALSE;

        return TRUE;

    }

    public function dropConstraint($name, $table, $cascade = false) {

        $sql = "ALTER TABLE " . $this->schemaTable($table) . " DROP CONSTRAINT " . $this->field($name) . ($cascade?' CASCADE':'');

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

        $sql = 'CREATE OR REPLACE VIEW ' . $this->field($name) . ' AS ' . rtrim($content, ' ;');

        return ($this->exec($sql) !== false);

    }

    public function dropView($name, $cascade = false){

        $sql = 'DROP VIEW ' . $this->field($name);

        if($cascade === true)
            $sql .= ' CASCADE';

        return ($this->exec($sql) !== false);

    }

    /**
     * List defined functions
     *
     * @return array
     */
    public function listFunctions($schema = null){

        if($schema === null)
            $schema = $this->schema;

        $sql = "SELECT r.routine_schema, r.routine_name FROM INFORMATION_SCHEMA.routines r WHERE r.specific_schema=" . $this->prepareValue($schema) . " AND routine_body != 'EXTERNAL'";

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

        if($schema === null)
            $schema = $this->schema;

        $sql = "SELECT r.specific_name,
                    r.routine_schema,
                    r.routine_name,
                    r.data_type AS return_type,
                    r.routine_body,
                    r.routine_definition,
                    r.external_language,
                    p.parameter_name,
                    p.data_type,
                    p.parameter_mode,
                    p.ordinal_position
                FROM INFORMATION_SCHEMA.routines r
                LEFT JOIN INFORMATION_SCHEMA.parameters p ON p.specific_name=r.specific_name
                WHERE r.routine_schema=" . $this->prepareValue($schema) . "
                AND r.routine_name=" . $this->prepareValue($name) ."
                ORDER BY r.routine_name, p.ordinal_position;";

        if(!($q = $this->query($sql)))
            throw new \Hazaar\Exception($this->errorInfo()[2]);

        $info = array();

        while($row = $q->fetch(\PDO::FETCH_ASSOC)){

            if(!array_key_exists($row['specific_name'], $info)){

                $item = array(
                    'schema' => $row['routine_schema'],
                    'name' => $row['routine_name'],
                    'return_type' => $row['return_type'],
                    'content' => trim($row['routine_definition'])
                );

                $item['parameters'] = array();

                $item['lang'] = (strtoupper($row['routine_body']) === 'EXTERNAL')
                    ? $row['external_language']
                    : $row['routine_body'];

                $info[$row['specific_name']] = $item;

            }

            if($row['parameter_name'] === null)
                continue;

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

    /**
     * Create a new database function
     *
     * @param mixed $name The name of the function to create
     * @param mixed $spec A function specification.  This is basically the array returned from describeFunction()
     * @return boolean
     */
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

    /**
     * Remove a function from the database
     *
     * @param mixed $name  The name of the function to remove
     * @param mixed $arg_types The argument list of the function to remove.
     * @param mixed $cascade Whether to perform a DROP CASCADE
     * @return boolean
     */
    public function dropFunction($name, $arg_types = array(), $cascade = false){

        $sql = 'DROP FUNCTION ' . $this->field($name);

        if($arg_types)
            $sql .= ' (' . (is_array($arg_types) ? implode(', ', $arg_types) : $arg_types) . ')';

        if($cascade === true)
            $sql .= ' CASCADE';

        return ($this->exec($sql) !== false);

    }

    /**
     * TRUNCATE � empty a table or set of tables
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

        $sql = 'TRUNCATE TABLE ' . ($only ? 'ONLY ' : '') . $this->prepareFields($table_name);

        $sql .= ' ' . ($restart_identity ? 'RESTART IDENTITY' : 'CONTINUE IDENTITY');

        $sql .=  ' ' . ($cascade ? 'CASCADE' : 'RESTRICT');

        return ($this->exec($sql) !== false);

    }

    /**
     * List defined triggers
     *
     * @param mixed $schema Optional: Schema name.  If not supplied the current schema is used.
     *
     * @return array
     */
    public function listTriggers($table = null, $schema = null){

        if($schema === null)
            $schema = $this->schema;

        $sql = 'SELECT DISTINCT trigger_schema AS schema, trigger_name AS name
                    FROM INFORMATION_SCHEMA.triggers
                    WHERE event_object_schema=' . $this->prepareValue($schema);

        if($table !== null)
            $sql .= ' AND event_object_table=' . $this->prepareValue($table);

        if($result = $this->query($sql))
            return $result->fetchAll(\PDO::FETCH_ASSOC);

        return null;

    }

    /**
     * Describe a database trigger
     *
     * This will return an array as there can be multiple triggers with the same name but with different attributes
     *
     * @param mixed $table Optional: The name of the table to describe triggers for
     * @param mixed $schema Optional: Schema name.  If not supplied the current schema is used.
     *
     * @return array
     */
    public function describeTrigger($name, $schema = null){

        if($schema === null)
            $schema = $this->schema;

        $sql = 'SELECT trigger_schema AS schema,
                        trigger_name AS name,
                        event_manipulation AS events,
                        event_object_table AS table,
                        action_statement AS content,
                        action_orientation AS orientation,
                        action_timing AS timing
                    FROM INFORMATION_SCHEMA.triggers
                    WHERE trigger_schema=' . $this->prepareValue($schema)
                    . ' AND trigger_name=' . $this->prepareValue($name);


        if(!($result = $this->query($sql)))
            return null;

        $info = $result->fetch(\PDO::FETCH_ASSOC);

        $info['events'] = array($info['events']);

        while($row = $result->fetch(\PDO::FETCH_ASSOC))
            $info['events'][] = $row['events'];

        return $info;

    }

    /**
     * Summary of createTrigger
     * @param mixed $name The name of the trigger
     * @param mixed $table The table on which the trigger is being created
     * @param mixed $spec The spec of the trigger.  Basically this is the array returned from describeTriggers()
     */
    public function createTrigger($name, $table, $spec = array()){

        $sql = 'CREATE TRIGGER ' . $this->field($name)
            . ' ' . ake($spec, 'timing', 'BEFORE')
            . ' ' . implode(' OR ', ake($spec, 'events', array('INSERT')))
            . ' ON ' . $this->schemaTable($table)
            . ' FOR EACH ' . ake($spec, 'orientation', 'ROW')
            . ' ' . ake($spec, 'content', 'EXECUTE');


        return ($this->exec($sql) !== false);

    }

    /**
     * Drop a trigger from a table
     *
     * @param mixed $name The name of the trigger to drop
     * @param mixed $table The name of the table to remove the trigger from
     * @param mixed $cascade Whether to drop CASCADE
     * @return boolean
     */
    public function dropTrigger($name, $table, $cascade = false){

        $sql = 'DROP TRIGGER ' . $this->field($name) . ' ON ' . $this->schemaTable($table);

        $sql .= ' ' . (($cascade === true) ? ' CASCADE' : ' RESTRICT');

        return ($this->exec($sql) !== false);

    }

}
