<?php

/**
 * @file        Hazaar/DBI/Result.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2012 Jamie Carl (http://www.hazaarlabs.com)
 */

/**
 * @brief Relational database namespace
 */
namespace Hazaar\DBI;

/**
 * @brief Relational Database Interface - Result Class
 */
class Result implements \ArrayAccess, \Countable, \Iterator {

    /**
     * The PDO statement object.
     *
     * @var $statement
     */
    private $statement;

    private $record;

    private $records;

    private $wakeup = false;

    /**
     * Flag to remember if we need to reset the statement when using array access methods.
     * A reset is required once an 'execute' is made and then rows are accessed. If no rows
     * are accessed then a reset is not required. This prevents a query from being executed
     * multiple times when it's not necessary.
     */
    private $reset = true;

    private $array_columns = array();

    private $type_map = array(
        'numeric'       => 'integer',
        'int2'          => 'integer',
        'int4'          => 'integer',
        'int8'          => 'integer',
        'float8'        => 'float',
        'timestamp'     => '\Hazaar\Date',
        'timestamptz'   => '\Hazaar\Date',
        'date'          => ['\Hazaar\Date', ['format' => 'Y-m-d']],
        'bool'          => 'boolean',
        'money'         => '\Hazaar\Money'
    );

    private $meta;

    private $adapter;

    private $encrypt = false;

    private $select_groups = array();
    
    function __construct(\Hazaar\DBI\Adapter $adapter, \PDOStatement $statement, $options = array()) {

        $this->adapter = $adapter;

        $this->statement = $statement;

        if(is_array($options))
            $this->encrypt =  ake($options, 'encrypt', false);

        $this->processStatement($statement);

    }

    public function setSelectGroups($select_groups){

        if(\is_array($select_groups))
            $this->select_groups = $select_groups;

    }

    private function processStatement(\PDOStatement $statement){

        if (!$statement instanceof \PDOStatement || $statement->rowCount() === 0)
            return false;

        $this->meta = array();

        for($i = 0; $i < $this->statement->columnCount(); $i++){

            $meta = $this->statement->getColumnMeta($i);

            $def = array('native_type' => $meta['native_type']);

            if(array_key_exists('table', $meta))
                $def['table'] = $meta['table'];

            if(substr($meta['native_type'], 0, 1) == '_'){

                if(!array_key_exists($meta['name'], $this->array_columns))
                    $this->array_columns[$meta['name']] = array();

                $this->array_columns[$meta['name']][] = substr($meta['native_type'], 1);

                $type = substr($meta['native_type'], 1);

                $def['type'] = 'array';

                $def['arrayOf'] =  ake($this->type_map, $type, 'string');

            }elseif ($meta['pdo_type'] == \PDO::PARAM_STR && (substr(ake($meta, 'native_type'), 0, 4) == 'json'
                    || (!array_key_exists('native_type', $meta) && in_array('blob', ake($meta, 'flags'))))){

                if(!array_key_exists($meta['name'], $this->array_columns))
                    $this->array_columns[$meta['name']] = array();
    
                $this->array_columns[$meta['name']][] = 'json';

                $def['prepare'] = function($value){ if(is_string($value)) return json_decode($value); return $value; };

            }else{

                $type_map = ake($this->type_map, $meta['native_type'], 'string');

                $extra = is_array($type_map) ? $type_map[1] : [];

                $type_map = is_array($type_map) ? $type_map[0] : $type_map;

                $def = array_merge($def, ['type' => $type_map], $extra);

            }

            if(array_key_exists($meta['name'], $this->meta)){

                if(!is_array($this->meta[$meta['name']]))
                    $this->meta[$meta['name']] = array($this->meta[$meta['name']]);

                $this->meta[$meta['name']][] = (object)$def;

            }else{

                $this->meta[$meta['name']] = (object)$def;

            }

        }

        return true;

    }

    public function __toString() {

        return $this->toString();

    }

    public function toString() {

        return $this->statement->queryString;

    }

    public function bindColumn($column, &$param, $type = null, $maxlen = null, $driverdata = null) {

        if ($this->statement instanceof \PDOStatement){

            if($driverdata !== null)
                return $this->statement->bindColumn($column, $param, $type, $maxlen, $driverdata);
            if($maxlen !== null)
                return $this->statement->bindColumn($column, $param, $type, $maxlen);
            if($type !== null)
                return $this->statement->bindColumn($column, $param, $type);

            return $this->statement->bindColumn($column, $param);

        }

        return false;

    }

    public function bindParam($parameter, &$variable, $data_type = \PDO::PARAM_STR, $length = null, $driver_options = null) {

        if ($this->statement instanceof \PDOStatement){

            if($driver_options !== null)
                return $this->statement->bindParam($parameter, $variable, $data_type, $length, $driver_options);
            if($length !== null)
                return $this->statement->bindParam($parameter, $variable, $data_type, $length);

            return $this->statement->bindParam($parameter, $variable, $data_type);

        }

        return false;

    }

    public function bindValue($parameter, $value, $data_type = \PDO::PARAM_STR) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->bindValue($parameter, $value, $data_type);

        return false;

    }

    public function closeCursor() {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->closeCursor();

        return false;

    }

    public function columnCount() {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->columnCount();

        return false;

    }

    public function debugDumpParams() {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->debugDumpParams();

        return false;

    }

    public function errorCode() {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->errorCode();

        return false;

    }

    public function errorInfo() {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->errorInfo();

        return false;

    }

    public function execute($input_parameters = array()) {

        $this->reset = false;

        if(is_array($input_parameters) && count($input_parameters) > 0)
            $result = $this->statement->execute($input_parameters);
        else
            $result = $this->statement->execute();

        if(!$result)
            return false;

        $this->processStatement($this->statement);

        if(preg_match('/^INSERT/i', $this->statement->queryString))
            return $this->adapter->lastInsertId();

        return $result;

    }

    public function fetch($fetch_style = \PDO::FETCH_ASSOC, $cursor_orientation = \PDO::FETCH_ORI_NEXT, $cursor_offset = 0) {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        if($record = $this->statement->fetch($fetch_style, $cursor_orientation, $cursor_offset)){

            $this->fix($record);

            return $record;

        }

        return null;

    }

    public function fetchAll($fetch_style = \PDO::FETCH_ASSOC, $fetch_argument = null, $ctor_args = array()) {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        if ($fetch_argument !== null)
            $results = $this->statement->fetchAll($fetch_style, $fetch_argument, $ctor_args);
        else
            $results = $this->statement->fetchAll($fetch_style);

        foreach($results as &$record) $this->fix($record);

        return $results;

    }

    public function fetchColumn($column_number = 0) {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        return $this->statement->fetchColumn($column_number);

    }

    public function fetchObject($class_name = "stdClass", $ctor_args) {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        return $this->statement->fetchObject($class_name, $ctor_args);

    }

    public function getAttribute($attribute) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->getAttribute($attribute);

        return false;

    }

    public function getColumnMeta($column) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->getColumnMeta($column);

        return false;

    }

    private function fix(&$record) {

        if (!$record)
            return null;

        if (!((count($this->array_columns) + count($this->select_groups)) > 0))
            return $record;

        foreach($this->array_columns as $col => $array_columns){

            if(count($array_columns) > 1)
                $columns =& $record[$col];
            else
                $columns = array(&$record[$col]);

            foreach($array_columns as $index => $type){

                if($columns[$index] === null)
                    continue;

                if($type == 'json'){

                    $columns[$index] = json_decode($columns[$index]);

                    continue;

                }

                if(!($columns[$index] && substr($columns[$index], 0, 1) == '{' && substr($columns[$index], -1, 1) == '}'))
                    continue;

                $elements = explode(',', trim($columns[$index], '{}'));

                foreach($elements as &$element){

                    if(substr($type, 0, 3) == 'int')
                        $element = intval($element);
                    elseif(substr($type, 0, 5) == 'float')
                        $element = floatval($element);
                    elseif($type == 'text' || $type == 'varchar')
                        $element = trim($element, "'");
                    elseif($type == 'bool')
                        $element = boolify($element);
                    elseif($type == 'timestamp' || $type == 'date' || $type == 'time')
                        $element = new \Hazaar\Date(trim($element, '"'));
                    elseif($type == 'json')
                        $element = json_decode($element);

                }

                $columns[$index] = $elements;

            }

            unset($columns);

        }

        $objs = array();

        foreach($record as $name => $value){

            if(array_key_exists($name, $this->select_groups)){

                $objs[$this->select_groups[$name]] = $value;

                unset($record[$name]);

                continue;

            }

            $aliases = array();

            $meta = null;

            if(is_array($this->meta[$name])){

                $meta = array();

                foreach($this->meta[$name] as $col){

                    $meta[] = $col;

                    $aliases[] = ake($col, 'table');

                }

            }else{

                $meta = $this->meta[$name];

                if(!($alias = ake($meta, 'table')))
                    continue;

                $aliases[] = $alias;

            }

            foreach($aliases as $idx => $alias){

                if(!array_key_exists($alias, $this->select_groups))
                    continue;

                while(array_key_exists($alias, $this->select_groups) && $this->select_groups[$alias] !== $alias)
                    $alias = $this->select_groups[$alias];

                if(!isset($objs[$alias]))
                    $objs[$alias] = array();

                $objs[$alias][$name] = (is_array($value) && is_array($meta)) ? $value[$idx] : $value;

                unset($record[$name]);

            }

        }

        $record = array_merge($record, array_from_dot_notation($objs));

        if($this->encrypt !== false)
            $this->decrypt($record);

        return $record;

    }

    public function nextRowset() {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        return $this->statement->nextRowset();

    }

    public function rowCount() {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->rowCount();

        return -1;

    }

    /*
     * Countable
     */
    public function count() {

        return $this->rowCount();

    }

    public function setAttribute($attribute, $value) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->setAttribute($attribute, $value);

        return false;

    }

    public function setFetchMode($mode) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->setFetchMode($mode);

        return false;

    }

    public function __get($key) {

        if (!$this->record) {

            $this->reset = true;

            $this->next();
        }

        return $this->record[$key];

    }

    public function all() {

        return $this->fetchAll();

    }

    public function row($cursor_orientation = \PDO::FETCH_ORI_NEXT, $offset = 0) {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        if($record = $this->statement->fetch(\PDO::FETCH_NAMED, $cursor_orientation, $offset))
            return new Row($this->adapter, $this->meta, $this->decrypt($record), $this->statement);

        return null;

    }

    public function rows() {

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        if($records = $this->statement->fetchAll(\PDO::FETCH_NAMED)){

            foreach($records as &$record)
                $record = new Row($this->adapter, $this->meta, $this->decrypt($record), $this->statement);

            return $records;

        }

        return null;

    }

    private function store() {

        if ($this->statement instanceof \PDOStatement && !$this->wakeup) {

            $this->records = $this->statement->fetchAll(\PDO::FETCH_NAMED);

            foreach($this->records as &$record) new Row($this->adapter, $this->meta, $this->decrypt($record), $this->statement);

            $this->wakeup = true;

            $this->reset = true;

        }

    }

    public function __sleep() {

        $this->store();

        return array('records');

    }

    public function __wakeup() {

        $this->wakeup = true;

    }

    /*
     * Array Access
     */
    public function offsetExists($offset) {

        if ($this->wakeup) {

            $record = current($this->records);

            return array_key_exists($offset, $record);
        }

        return (is_array($this->record) && array_key_exists($offset, $this->record));

    }

    public function offsetGet($offset) {

        if ($this->wakeup) {

            $record = current($this->records);

            return $record[$offset];
        }

        return $this->__get($offset);

    }

    public function offsetSet($offset, $value) {

        throw new \Hazaar\Exception('Updating a value in a database result is not supported!');

    }

    public function offsetUnset($offset) {

        throw new \Hazaar\Exception('Unsetting a value in a database result is not supported!');

    }

    /*
     * Iterator
     */
    public function current() {

        if ($this->wakeup)
            return current($this->records);

        return $this->record;

    }

    public function key() {

        return key($this->record);

    }

    public function next() {

        if ($this->wakeup)
            return next($this->records);

        $this->record = null;

        if (!$this->statement instanceof \PDOStatement)
            return false;

        $this->reset = true;

        if($record = $this->statement->fetch(\PDO::FETCH_NAMED, \PDO::FETCH_ORI_NEXT))
            return $this->record = new Row($this->adapter, $this->meta, $this->decrypt($record), $this->statement);

        return null;

    }

    public function rewind() {

        if ($this->wakeup)
            return reset($this->records);

        $this->record = null;

        if (!$this->statement instanceof \PDOStatement)
            return false;

        if($this->reset === true)
            $this->statement->execute();

        $this->reset = false;

        if($record = $this->statement->fetch(\PDO::FETCH_NAMED, \PDO::FETCH_ORI_NEXT))
            return $this->record = new Row($this->adapter, $this->meta, $this->decrypt($record), $this->statement);

        return null;

    }

    public function valid() {

        if ($this->wakeup)
            return (current($this->records));

        return $this->record instanceof Row;

    }

    private function decrypt(&$data){

        if($data === null
            || !(is_array($data) && count($data) > 0)
            || $this->encrypt === false)
            return $data;

        $cipher = ake($this->encrypt, 'cipher', 'aes-256-ctr');

        $key = ake($this->encrypt, 'key', '0000');

        $checkstring = ake($this->encrypt, 'checkstring', Adapter::$default_checkstring);

        foreach($data as $key => &$value){

            if(!(in_array($key, ake($this->encrypt['table'], $this->meta[$key]['table'], array())) && $this->meta[$key]['type'] === 'string'))
                continue;

            $parts = preg_split('/(?<=.{' . openssl_cipher_iv_length($cipher) . '})/', base64_decode($value), 2);

            if(count($parts) !== 2)
                continue;

            list($checkbit, $value) = preg_split('/(?<=.{' . strlen($checkstring) . '})/', openssl_decrypt($parts[1], $cipher, $key, OPENSSL_RAW_DATA, $parts[0]), 2);

            if($checkbit !== $checkstring)
                throw new \Hazaar\Exception('Field decryption failed: ' . $key);

        }

        return $data;

    }

    /**
     * Collates a result into a simple key/value array.
     *
     * This is useful for generating SELECT lists directly from a resultset.
     *
     * @param mixed $index_column The column to use as the array index.
     * @param mixed $value_column The column to use as the array value.
     * @param mixed $group_column Optional column name to group items by.
     *
     * @return array
     */
    public function collate($index_column, $value_column, $group_column = null){

        return array_collate($this->fetchAll(), $index_column, $value_column, $group_column);

    }

}
