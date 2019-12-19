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
 * ```php
 * $db = new Hazaar\DBI();
 * $result = $db->users->find(array('uname' => 'myusername'))->join('images', array('image' => array('$ref' => 'images.id')));
 * while($row = $result->fetch()){
 * //Do things with $row here
 * }
 * ```
 */
class Table {

    private $adapter;

    private $name;

    private $alias;

    private $criteria = array();

    private $fields = array();

    private $group = array();

    private $having = array();

    private $window = array();

    private $joins = array();

    private $order = array();

    private $limit;

    private $offset;

    private $result;

    private $options;

    function __construct(Adapter $adapter, $name, $alias = NULL, $options = null) {

        $this->adapter = $adapter;

        $this->name = $name;

        $this->alias = $alias;

        $this->options = $options;

    }

    private function from() {

        return $this->adapter->quoteSpecial($this->name) . ($this->alias ? ' ' . $this->alias : NULL);

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
            return $result->fetch();

        return FALSE;

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
    public function findOneRow($criteria = array(), $fields = array(), $order = NULL) {

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
            return $this->adapter->tableExists($this->name);

        if($criteria !== null)
            $sql = 'SELECT EXISTS (SELECT * FROM ' . $this->from() . ' WHERE ' . $this->adapter->prepareCriteria($criteria) . ');';
        else
            $sql = 'SELECT EXISTS (' . $this->toString(false) . ');';

        if (!($result = $this->adapter->query($sql)))
            throw $this->adapter->errorException();

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
    public function toString($terminate_with_colon = FALSE) {

        $sql = 'SELECT';

        if (!is_array($this->fields) || count($this->fields) == 0)
            $sql .= ' *';
        else
            $sql .= ' ' . $this->adapter->prepareFields($this->fields, null, $this->tables());

        /* FROM */
        $sql .= ' FROM ' . $this->from();

        if (count($this->joins) > 0) {

            foreach($this->joins as $join) {

                $sql .= ' ' . $join['type'] . ' JOIN ' . $this->adapter->field($join['ref']);

                if ($join['alias'])
                    $sql .= ' ' . $join['alias'];

                $sql .= ' ON ' . $this->adapter->prepareCriteria($join['on']);
            }
        }

        /* WHERE */
        if(is_array($this->criteria) && count($this->criteria) > 0)
            $sql .= ' WHERE ' . $this->adapter->prepareCriteria($this->criteria);

        /* GROUP BY */
        if(count($this->group) > 0)
            $sql .= ' GROUP BY ' . $this->adapter->prepareFields($this->group);

        /* HAVING */
        if (count($this->having) > 0)
            $sql .= ' HAVING ' . $this->adapter->prepareCriteria($this->having);

        /* WINDOW */
        if(count($this->window) > 0){

            $items = array();

            foreach($this->window as $name => $info){

                $item = 'PARTITION BY ' . $this->adapter->prepareFields((array)$info['as']);

                if($info['order'])
                    $item .= ' ORDER BY ' . $this->prepareOrder($info['order']);

                $items[] = $name . ' AS ( ' . $item . ' )';

            }

            $sql .= ' WINDOW ' . implode(', ', $items);

        }

        /* ORDER BY */
        if (count($this->order) > 0)
            $sql .= ' ORDER BY ' . $this->prepareOrder($this->order);

        /* LIMIT */
        if ($this->limit !== NULL)
            $sql .= ' LIMIT ' . (string) (int) $this->limit;

        /* OFFSET */
        if ($this->offset !== NULL)
            $sql .= ' OFFSET ' . (string) (int) $this->offset;

        /* FETCH */

        /* FOR */


        if ($terminate_with_colon)
            $sql .= ';';

        return $sql;

    }

    private function tables(){

        $alias = ($this->alias) ? $this->alias : $this->name;

        $tables = array($alias => $this->name);

        foreach($this->joins as $alias => $join)
            $tables[$alias] = $join['ref'];

        return $tables;

    }

    private function prepareOrder($order_definition){

        $order = array();

        if(is_string($order_definition)){

            $order[] = $order_definition;

        }elseif(is_array($order_definition)){

            foreach($order_definition as $field => $mode) {

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

        }

        return implode(', ', $order);

    }

    /**
     * Execute the current selection
     *
     * @throws \Exception
     * @return Result
     */
    public function execute() {

        if ($this->result === null) {

            DBD\BaseDriver::$select_groups = array();

            $sql = $this->toString();

            if (!($this->result = $this->adapter->query($sql)))
                throw $this->adapter->errorException();

            $this->result->setSelectGroups(DBD\BaseDriver::$select_groups);

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
     * @return Result|boolean
     */
    public function prepare($criteria = array(), $fields = array(), $name = null){

        $this->find($criteria, $fields);

        if ($statement = $this->adapter->prepare($this->toString(), $name))
            return new Result($this->adapter, $statement, $this->options);

        return false;

    }

    /**
     * Defined the current field selection definition
     *
     * @param mixed $fields A valid field definition
     * @return Table
     */
    public function fields() {

        $this->fields[] = func_get_args();

        return $this;

    }

    /**
     * Alias for Hazaar\DBI\Table::fields()
     *
     * @param mixed $fields One or more column names
     * @return Table
     */
    public function select(){

        return $this->fields(func_get_args());

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

    public function group(){

        $this->group = array_merge($this->group, func_get_args());

        return $this;

    }

    public function having(){

        $this->having = array_merge($this->having, func_get_args());

        return $this;

    }

    public function window($name, $partition_by, $order_by = null){

        $this->window[$name] = array(
            'as' => $partition_by,
            'order' => $order_by
        );

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
    public function join($references, $on, $alias = NULL, $type = 'INNER') {

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

    public function limit($limit = null) {

        if($limit === null)
            return $this->limit;
            
        $this->limit = $limit;

        return $this;

    }

    public function offset($offset) {

        $this->offset = $offset;

        return $this;

    }

    public function insert($fields, $returning = NULL) {

        return $this->adapter->insert($this->name, $fields, $returning);

    }

    public function update($criteria, $fields) {

        $from = array();

        if(count($this->joins) > 0){

            foreach($this->joins as $join){

                $from[] = $join['ref'] . (array_key_exists('alias', $join) ? ' ' . $join['alias'] : null);

                $criteria[] = $join['on'];

            }

        }

        $name = $this->adapter->field($this->name) . ($this->alias ? ' ' . $this->alias : null);

        return $this->adapter->update($name, $fields, $criteria, $from);

    }

    public function delete($criteria) {

        $from = array();

        if(count($this->joins) > 0){

            foreach($this->joins as $join){

                $from[] = $join['ref'] . (array_key_exists('alias', $join) ? ' ' . $join['alias'] : null);

                $criteria[] = $join['on'];

            }

        }

        $name = $this->adapter->field($this->name) . ($this->alias ? ' ' . $this->alias : null);

        return $this->adapter->delete($name , $criteria, $from);

    }

    public function deleteAll() {

        return $this->adapter->deleteAll($this->name);

    }

    public function row($offset = 0) {

        if ($result = $this->execute())
            return $result->row($offset);

        return FALSE;

    }

    public function rows() {

        if ($result = $this->execute())
            return $result->rows();

        return FALSE;

    }

    public function fetch($cursor_orientation = \PDO::FETCH_ORI_NEXT, $offset = 0) {

        if ($result = $this->execute())
            return $result->fetch((is_assoc($this->fields) ? \PDO::FETCH_NAMED : \PDO::FETCH_ASSOC), $cursor_orientation, $offset);

        return FALSE;

    }

    public function fetchAll($fetch_argument = null, $ctor_args = array()) {

        if ($result = $this->execute())
            return $result->fetchAll((is_assoc($this->fields) ? \PDO::FETCH_NAMED : \PDO::FETCH_ASSOC), $fetch_argument, $ctor_args);

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

        throw new \Hazaar\Exception('Updating a value in a database result is not currently implemented!');

    }

    public function offsetUnset($offset) {

        throw new \Hazaar\Exception('Unsetting a value in a database result is not currently implemented!');

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

                    $sql .= ' ' . $join['type'] . ' JOIN ' . $this->adapter->field($join['ref']);

                    if ($join['alias'])
                        $sql .= ' ' . $join['alias'];

                    $sql .= ' ON ' . $this->adapter->prepareCriteria($join['on']);
                }
            }

            if ($this->criteria)
                $sql .= ' WHERE ' . $this->adapter->prepareCriteria($this->criteria);

            if ($result = $this->adapter->query($sql))
                return (int)$result->fetchColumn(0);

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

        return array_collate($this->fetchAll(), $index_column, $value_column, $group_column);

    }

    /**
     * Truncate the table
     *
     * Truncating a table quickly removes all rows from a set of tables. It has the same effect as Hazaar\DBI\Table::deleteAll() on
     * each table, but since it does not actually scan the tables it is faster. Furthermore, it reclaims disk space
     * immediately, rather than requiring a subsequent VACUUM operation. This is most useful on large tables.
     *
     * @param mixed $only               Only the named table is truncated. If FALSE, the table and all its descendant tables (if any) are truncated.
     * @param mixed $restart_identity   Automatically restart sequences owned by columns of the truncated table(s).  The default is to no restart.
     * @param mixed $cascade            If TRUE, automatically truncate all tables that have foreign-key references to any of the named tables, or
     *                                  to any tables added to the group due to CASCADE.  If FALSE, Refuse to truncate if any of the tables have
     *                                  foreign-key references from tables that are not listed in the command. FALSE is the default.
     * @return boolean
     */
    public function truncate($only = false, $restart_identity = false, $cascade = false){

        return $this->adapter->truncate($this->name, $only, $restart_identity, $cascade);

    }

}