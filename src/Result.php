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
     * A reset is required once an
     * 'execute' is made and then rows are accessed. If no rows are accessed then a reset is not required. This
     * prevents a query from being executed multiple times when it's not necessary.
     */
    private $reset = true;

    private $array_columns = array();

    private $json_columns = array();

    function __construct(\PDOStatement $statement) {

        $this->statement = $statement;

        if ($statement instanceof \PDOStatement) {

            for($i = 0; $i < $this->statement->columnCount(); $i++) {

                $meta = $this->statement->getColumnMeta($i);

                if(substr($meta['native_type'], 0, 1) == '_'){ //It's an array!

                    $this->array_columns[] = array(substr($meta['native_type'], 1), $meta['name']);

                }elseif ($meta['pdo_type'] == \PDO::PARAM_STR && (substr(ake($meta, 'native_type'), 0, 4) == 'json' || (!array_key_exists('native_type', $meta) && in_array('blob', ake($meta, 'flags'))))){

                    $this->json_columns[] = $meta['name'];

                }


            }

        }

    }

    public function __toString() {

        return $this->toString();

    }

    public function toString() {

        dump($this->statement);

        return $this->statement->queryString;

    }

    public function bindColumn($column, &$param, $type, $maxlen, $driverdata) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->bindColumn($column, $param, $type, $maxlen, $driverdata);

        return false;

    }

    public function bindParam($parameter, &$variable, $data_type = \PDO::PARAM_STR, $length, $driver_options) {

        if ($this->statement instanceof \PDOStatement)
            return $this->statement->bindParam($parameter, $variable, $data_type, $length, $driver_options);

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

    public function execute($input_parameters) {

        if (!is_array($input_parameters))
            $input_parameters = array(
                $input_parameters
            );

        $this->reset = false;

        return $this->statement->execute($input_parameters);

    }

    public function fetch($fetch_style = \PDO::FETCH_ASSOC, $cursor_orientation = \PDO::FETCH_ORI_NEXT, $cursor_offset = 0) {

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            $record = $this->statement->fetch($fetch_style, $cursor_orientation, $cursor_offset);

            return $this->fix($record);

        }

        return false;

    }

    public function fetchAll($fetch_style = \PDO::FETCH_ASSOC, $fetch_argument = null, $ctor_args = array()) {

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            if ($fetch_argument !== null) {

                return $this->statement->fetchAll($fetch_style, $fetch_argument, $ctor_args);
            }

            return $this->statement->fetchAll($fetch_style);
        }

        return false;

    }

    public function fetchColumn($column_number = 0) {

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            return $this->statement->fetchColumn($column_number);
        }

        return false;

    }

    public function fetchObject($class_name = "stdClass", $ctor_args) {

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            return $this->statement->fetchObject($class_name, $ctor_args);
        }

        return false;

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

        if (count($this->array_columns) > 0) {

            foreach($this->array_columns as $item){

                list($type, $col) = $item;

                if(!($record[$col] && substr($record[$col], 0, 1) == '{' && substr($record[$col], -1, 1) == '}'))
                    continue;

                $elements = explode(',', trim($record[$col], '{}'));

                foreach($elements as &$element){

                    if(substr($type, 0, 3) == 'int')
                        $element = intval($element);

                }

                $record[$col] = $elements;

            }

        }

        if (count($this->json_columns) > 0) {

            foreach($this->json_columns as $col)
                $record[$col] = json_decode($record[$col], true);

        }

        return $record;

    }

    public function nextRowset() {

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            return $this->statement->nextRowset();
        }

        return false;

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

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            return $this->statement->fetchAll(\PDO::FETCH_ASSOC);
        }

        return false;

    }

    public function row() {

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            $record = $this->statement->fetch(\PDO::FETCH_ASSOC);

            return $this->fix($record);
        }

        return false;

    }

    private function store() {

        if ($this->statement instanceof \PDOStatement && !$this->wakeup) {

            $this->records = $this->statement->fetchAll(\PDO::FETCH_ASSOC);

            foreach($this->records as &$record)
                $this->fix($record);

            $this->wakeup = true;

            $this->reset = true;
        }

    }

    public function __sleep() {

        $this->store();

        return array(
            'records'
        );

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

        throw new \Exception('Updating a value in a database result is not supported!');

    }

    public function offsetUnset($offset) {

        throw new \Exception('Unsetting a value in a database result is not supported!');

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

        if ($this->statement instanceof \PDOStatement) {

            $this->reset = true;

            $this->record = $this->statement->fetch(\PDO::FETCH_ASSOC);

            return $this->fix($this->record);
        }

        return false;

    }

    public function rewind() {

        if ($this->wakeup)
            return reset($this->records);

        if ($this->statement instanceof \PDOStatement) {

            if ($this->reset == true)
                $this->statement->execute();

            $this->record = $this->statement->fetch(\PDO::FETCH_ASSOC);

            return $this->fix($this->record);
        }

        return false;

    }

    public function valid() {

        if ($this->wakeup)
            return (current($this->records));

        return is_array($this->record);

    }

}

