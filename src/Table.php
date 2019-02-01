<?php

/**
 * @file        Hazaar/DBI/Collection.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2012 Jamie Carl (http://www.hazaarlabs.com)
 */
namespace Hazaar\DBI;

/**
 * @brief Relational Database Interface - Table Class
 *
 * @detail The Table class is used to access table data via an abstracted interface. That means that now SQL is
 * used to access table data and queries are generated automatically using access methods. The generation
 * of SQL is then handled by the database driver so that database specific SQL can be used when required.
 * This allows a common interface for accessing data that is compatible across all of the database drivers.
 *
 * h2. Example Usage
 *
 * <code>
 * $db = new Hazaar\DBI();
 * $result = $db->users->find(array('uname' => 'myusername'))->join('images', array('image' => array('$ref' => 'images.id')));
 * while($row = $result->fetch()){
 * //Do things with $row here
 * }
 * </code>
 */
class Table {

    private $driver;

    private $name;

    private $alias;

    private $criteria = array();

    private $fields = array();

    private $group = array();

    private $having = array();

    private $joins = array();

    private $order;

    private $limit;

    private $offset;

    private $result;

    private $options;

    private $encrypt = false;

    static private $default_checkstring = '!!';

    function __construct(DBD\BaseDriver $driver, $name, $alias = NULL, $options = null) {

        $this->driver = $driver;

        $this->name = $name;

        $this->alias = $alias;

        $this->options = $options;

        $this->encrypt =  ake($this->options, 'encrypt');

    }

    private function from() {

        return $this->driver->quoteSpecial($this->name) . ($this->alias ? ' ' . $this->alias : NULL);

    }

    /**
     * Search for records on a table with the provided search criteria
     *
     * @param mixed $criteria The search criteria to find records for.
     * @param mixed $fields A field definition.
     * @return Table
     */
    public function find($criteria = array(), $fields = array()) {

        if (!is_array($criteria))
            $criteria = array($criteria);

        $this->criteria = $criteria;

        if (!is_array($fields))
            $fields = array($fields);

        if(is_array($fields) && count($fields) > 0)
            $this->fields = $fields;

        return $this;

    }

    /**
     * Find a single row using the provided criteria, fields and order and return is as an array.
     *
     * @param mixed $criteria The search criteria.
     *
     * @param mixed $fields A field definition array.
     *
     * @param mixed $order A valid order definition
     *
     * @return mixed
     */
    public function findOne($criteria = array(), $fields = array(), $order = NULL) {

        if ($result = $this->find($criteria, $fields, $order, 1))
            return $result->row();

        return FALSE;

    }

    /**
     * Check if rows exist in the database
     *
     * @param mixed $criteria The search criteria to check for existing rows.
     *
     * @return bool
     */
    public function exists($criteria = null) {

        if($criteria === null && !$this->criteria)
            return $this->driver->tableExists($this->name);

        if($criteria !== null)
            $sql = 'SELECT EXISTS (SELECT * FROM ' . $this->from() . ' WHERE ' . $this->driver->prepareCriteria($criteria) . ');';
        else
            $sql = 'SELECT EXISTS (' . $this->toString(false) . ');';

        if (!($result = $this->driver->query($sql)))
            throw new \Exception($this->driver->errorInfo()[2]);

        return boolify($result->fetchColumn(0));

    }

    public function __tostring() {

        return $this->toString();

    }

    /**
     * Return the current selection as a valid SQL string
     *
     * @param mixed $terminate_with_colon
     *
     * @return string
     */
    public function toString($terminate_with_colon = TRUE) {

        $sql = 'SELECT';

        if (!is_array($this->fields) || count($this->fields) == 0)
            $sql .= ' *';
        else
            $sql .= ' ' . $this->driver->prepareFields($this->fields);

        $sql .= ' FROM ' . $this->from();

        if (count($this->joins) > 0) {

            foreach($this->joins as $join) {

                $sql .= ' ' . $join['type'] . ' JOIN ' . $this->driver->field($join['ref']);

                if ($join['alias'])
                    $sql .= ' ' . $join['alias'];

                $sql .= ' ON ' . $this->driver->prepareCriteria($join['on']);
            }
        }

        if(is_array($this->criteria) && count($this->criteria) > 0)
            $sql .= ' WHERE ' . $this->driver->prepareCriteria($this->criteria);

        if ($this->order) {

            $sql .= ' ORDER BY ';

            $order = array();

            foreach($this->order as $field => $mode) {

                if (is_array($mode)) {

                    $nulls = ake($mode, '$nulls', 0);

                    $mode = ake($mode, '$dir', 1);
                } else {

                    $nulls = 0;
                }

                $dir = (($mode == 1) ? 'ASC' : 'DESC');

                if ($nulls > 0)
                    $dir .= ' NULLS FIRST';

                elseif ($nulls < 0)
                    $dir .= ' NULLS LAST';

                $order[] = $field . ($dir ? ' ' . $dir : NULL);
            }

            $sql .= implode(', ', $order);
        }

        if(count($this->group) > 0)
            $sql .= ' GROUP BY ' . $this->driver->prepareFields($this->group);

        if (count($this->having) > 0)
            $sql .= ' HAVING ' . $this->driver->prepareCriteria($this->having);

        if ($this->limit !== NULL)
            $sql .= ' LIMIT ' . (string) (int) $this->limit;

        if ($this->offset !== NULL)
            $sql .= ' OFFSET ' . (string) (int) $this->offset;

        if ($terminate_with_colon)
            $sql .= ';';

        return $sql;

    }

    /**
     * Execute the current selection
     *
     * @throws \Exception
     * @return Result
     */
    public function execute() {

        if ($this->result === null) {

            $sql = $this->toString();

            if ($stmt = $this->driver->query($sql))
                $this->result = new Result($stmt);
            else
                throw new \Exception($this->driver->errorinfo()[2]);
        }

        return $this->result;

    }

    /**
     * Prepare a statement for execution and returns a new \Hazaar\Result object
     *
     * The criteria can contain zero or more names (:name) or question mark (?) parameter markers for which
     * real values will be substituted when the statement is executed. Both named and question mark parameter
     * markers cannot be used within the same statement template; only one or the other parameter style. Use
     * these parameters to bind any user-input, do not include the user-input directly in the query.
     *
     * You must include a unique parameter marker for each value you wish to pass in to the statement when you
     * call \Hazaar\Result::execute(). You cannot use a named parameter marker of the same name more than once
     * in a prepared statement.
     *
     * @param mixed $criteria   The query selection criteria.
     * @param mixed $fields     The field selection.
     * @throws \Exception
     * @return null
     */
    public function prepare($criteria = array(), $fields = array()){

        $this->find($criteria, $fields);

        if ($stmt = $this->driver->prepare($this->toString()))
            $this->result = new Result($stmt);
        else
            throw new \Exception($this->driver->errorinfo()[2]);

        return $this->result;

    }

    /**
     * Defined the current field selection definition
     *
     * @param mixed $fields A valid field definition
     * @return Table
     */
    public function fields($fields) {

        $this->fields = array_merge($this->fields, (array)$fields);

        return $this;

    }

    /**
     * Alias for Hazaar\DBI\Table::fields()
     *
     * @param mixed $fields One or more column names
     * @return Table
     */
    public function select($fields){

        return $this->fields($fields);

    }

    /**
     * Defines a WHERE selection criteria
     * @param mixed $criteria
     * @return Table
     */
    public function where($criteria) {

        if(is_string($criteria))
            $this->criteria[] = $criteria;
        else
            $this->criteria = array_merge($this->criteria, (array)$criteria);

        return $this;

    }

    public function group($columns){

        $this->group = array_merge($this->group, (array)$columns);

        return $this;

    }

    public function having($criteria){

        $this->having = array_merge($this->having, (array)$criteria);

        return $this;

    }

    /**
     * Join a table to the current query using the provided join criteria.
     *
     * @param string $references The table to join to the query.
     * @param array $on The join criteria.  This is mostly just a standard query selection criteria.
     * @param string $alias An alias to use for the joined table.
     * @param string $type The join type such as INNER, OUTER, LEFT, RIGHT, etc.
     * @return Table
     */
    public function join($references, $on = array(), $alias = NULL, $type = 'INNER') {

        if (!$type)
            $type = 'INNER';

        $this->joins[$alias] = array(
            'type' => $type,
            'ref' => $references,
            'on' => $on,
            'alias' => $alias
        );

        return $this;

    }

    public function innerJoin($references, $on = array(), $alias = NULL) {

        return $this->join($references, $on, $alias, 'INNER');

    }

    public function leftJoin($references, $on = array(), $alias = NULL) {

        return $this->join($references, $on, $alias, 'LEFT');

    }

    public function rightJoin($references, $on = array(), $alias = NULL) {

        return $this->join($references, $on, $alias, 'RIGHT');

    }

    function fullJoin($references, $on = array(), $alias = NULL) {

        return $this->join($references, $on, $alias, 'FULL');

    }

    public function sort($field_def, $desc = FALSE) {

        if (!is_array($field_def)) {

            $field_def = array(
                $field_def => ($desc ? -1 : 1)
            );
        }

        $this->order = $field_def;

        return $this;

    }

    public function limit($limit = 1) {

        $this->limit = $limit;

        return $this;

    }

    public function offset($offset) {

        $this->offset = $offset;

        return $this;

    }

    public function insert($fields, $returning = NULL) {

        return $this->driver->insert($this->name, $this->encrypt($fields), $returning);

    }

    public function update($criteria, $fields) {

        return $this->driver->update($this->name, $this->encrypt($fields), $criteria);

    }

    public function delete($criteria) {

        return $this->driver->delete($this->name, $criteria);

    }

    public function deleteAll() {

        return $this->driver->deleteAll($this->name);

    }

    public function row() {

        return $this->fetch();

    }

    public function all() {

        return $this->fetchAll();

    }

    public function fetch($offset = 0) {

        if ($result = $this->execute()){

            $data = $result->fetch(\PDO::FETCH_ASSOC, \PDO::FETCH_ORI_NEXT, $offset);

            return $this->decrypt($data, $result);

        }

        return FALSE;

    }

    public function fetchAll() {

        if ($result = $this->execute()){

            $data = $result->fetchAll(\PDO::FETCH_ASSOC);

            if(is_array($data)) foreach($data as &$row) $this->decrypt($row, $result);

            return $data;

        }

        return FALSE;

    }

    public function reset() {

        $this->result = NULL;

        return $this;

    }

    /*
     * Array Access
     */
    public function offsetExists($offset) {

        if ($result = $this->execute())
            return array_key_exists($offset, $result);

        return FALSE;

    }

    public function offsetGet($offset) {

        if ($result = $this->execute())
            return $result[$offset];

        return NULL;

    }

    public function offsetSet($offset, $value) {

        throw new \Exception('Updating a value in a database result is not currently implemented!');

    }

    public function offsetUnset($offset) {

        throw new \Exception('Unsetting a value in a database result is not currently implemented!');

    }

    /*
     * Iterator
     */
    public function current() {

        if (!$this->result)
            $this->execute();

        return $this->result->current();

    }

    public function key() {

        if (!$this->result)
            $this->execute();

        return $this->result->key();

    }

    public function next() {

        if (!$this->result)
            $this->execute();

        return $this->result->next();

    }

    public function rewind() {

        if (!$this->result)
            $this->execute();

        return $this->result->rewind();

    }

    public function valid() {

        if (!$this->result)
            $this->execute();

        return $this->result->valid();

    }

    /*
     * Countable
     */
    public function count() {

        if ($this->result) {

            return $this->result->rowCount();

        } else {

            $sql = 'SELECT count(*) FROM ' . $this->from();

            if (count($this->joins) > 0) {

                foreach($this->joins as $join) {

                    $sql .= ' ' . $join['type'] . ' JOIN ' . $this->driver->field($join['ref']);

                    if ($join['alias'])
                        $sql .= ' ' . $join['alias'];

                    $sql .= ' ON ' . $this->driver->prepareCriteria($join['on']);
                }
            }

            if ($this->criteria)
                $sql .= ' WHERE ' . $this->driver->prepareCriteria($this->criteria);

            if ($stmt = $this->driver->query($sql)) {

                $result = new Result($stmt);

                return (int)$result->fetchColumn(0);

            }

        }

        return FALSE;

    }

    public function getResult() {

        if (!$this->result)
            $this->execute();

        return $this->result;

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

        if (!$this->result)
            $this->execute();

        return array_collate($this->fetchAll(), $index_column, $value_column, $group_column);

    }

    private function encrypted(&$fields = array()){

        if($this->encrypt
            && array_key_exists('table', $this->options['encrypt'])
            && array_key_exists($this->name, $this->options['encrypt']['table'])){

            return (is_array($this->options['encrypt']['table'][$this->name])
                    ? array_intersect($this->options['encrypt']['table'][$this->name], array_keys($fields))
                    : ($this->options['encrypt']['table'][$this->name] === true ? array_keys($fields) : null));

        }

        return false;

    }

    private function encrypt(&$data){

        if(($encrypt_fields = $this->encrypted($data)) === false)
            return $data;

        $cipher = ake($this->options['encrypt'], 'cipher', 'aes-256-ctr');

        $key = ake($this->options['encrypt'], 'key', '0000');

        $checkstring = ake($this->options['encrypt'], 'checkstring', Table::$default_checkstring);

        foreach($encrypt_fields as $field){

            if(!is_string($field))
                throw new \Exception('Trying to encrypt non-string field: ' . $field);

            $iv = openssl_random_pseudo_bytes(openssl_cipher_iv_length($cipher));

            $content = base64_encode($iv . openssl_encrypt($checkstring . $data[$field], $cipher, $key, OPENSSL_RAW_DATA, $iv));

            $data[$field] = $content;

        }

        return $data;

    }

    private function decrypt(&$data, Result $result = null){

        if($data === null || ($encrypted_fields = $this->encrypted($data)) === false)
            return $data;

        $cipher = ake($this->options['encrypt'], 'cipher', 'aes-256-ctr');

        $key = ake($this->options['encrypt'], 'key', '0000');

        $checkstring = ake($this->options['encrypt'], 'checkstring', Table::$default_checkstring);

        $strings = null;

        if($result !== null){

            $strings = array();

            for($i = 0; $i < $result->columnCount(); $i++){

                $meta = $result->getColumnMeta($i);

                if($meta['native_type'] === 'text')
                    $strings[] = $meta['name'];

            }

        }

        foreach($encrypted_fields as $field){

            if(!($strings !== null && in_array($field, $strings) || ($strings === null && is_string($data[$field]))))
                continue;

            list($iv, $content) = preg_split('/(?<=.{' . openssl_cipher_iv_length($cipher) . '})/', base64_decode($data[$field]), 2);

            list($checkbit, $content) = preg_split('/(?<=.{' . strlen($checkstring) . '})/', openssl_decrypt($content, $cipher, $key, OPENSSL_RAW_DATA, $iv), 2);

            if($checkbit !== $checkstring)
                throw new \Exception('Field decryption failed: ' . $field);

            $data[$field] = $content;

        }

        return $data;

    }

}