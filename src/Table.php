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

    private $joins = array();

    private $order;

    private $limit;

    private $offset;

    private $result;

    function __construct(DBD\BaseDriver $driver, $name, $alias = NULL) {

        $this->driver = $driver;

        $this->name = $name;

        $this->alias = $alias;

    }

    private function from() {

        return $this->driver->quoteSpecial($this->name) . ($this->alias ? ' ' . $this->alias : NULL);

    }

    /**
     *
     * @param array $criteria
     * @param array $fields
     *
     * @return \Hazaar\DBI\Table
     */
    public function find($criteria = array(), $fields = array()) {

        if (!is_array($criteria))
            $criteria = array();

        $this->criteria = $criteria;

        if (!is_array($fields))
            $fields = array(
                $fields
            );

        $this->fields = $fields;

        return $this;

    }

    public function findOne($criteria = array(), $fields = array(), $order = NULL) {

        if ($result = $this->find($criteria, $fields, $order, 1))
            return $result->row();

        return FALSE;

    }

    public function exists($criteria = null) {

        if ($criteria) {

            $sql = 'SELECT EXISTS (SELECT * FROM ' . $this->from() . ' WHERE ' . $this->driver->prepareCriteria($criteria) . ');';

            if ($result = $this->driver->query($sql))
                return boolify($result->fetchColumn(0));

            return FALSE;
        }

        return $this->driver->tableExists($this->name);

    }

    public function __tostring() {

        return $this->toString();

    }

    public function toString($terminate_with_colon = TRUE) {

        $sql = 'SELECT';

        if (!is_array($this->fields) || count($this->fields) == 0) {

            $sql .= ' *';
        } else {

            $sql .= ' ' . $this->driver->prepareFields($this->fields);
        }

        $sql .= ' FROM ' . $this->from();

        if (count($this->joins) > 0) {

            foreach($this->joins as $join) {

                $sql .= ' ' . $join['type'] . ' JOIN ' . $this->driver->field($join['ref']);

                if ($join['alias'])
                    $sql .= ' ' . $join['alias'];

                $sql .= ' ON ' . $this->driver->prepareCriteria($join['on']);
            }
        }

        if (count($this->criteria) > 0)
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

        if ($this->limit !== NULL)
            $sql .= ' LIMIT ' . (string) (int) $this->limit;

        if ($this->offset !== NULL)
            $sql .= ' OFFSET ' . (string) (int) $this->offset;

        if ($terminate_with_colon)
            $sql .= ';';

        return $sql;

    }

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

    public function fields($fields) {

        $this->fields = array_merge($this->fields, $fields);

        return $this;

    }

    public function where($criteria) {

        $this->criteria = array_merge($this->criteria, $criteria);

        return $this;

    }

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

        return $this->driver->insert($this->name, $fields, $returning);

    }

    public function update($criteria, $fields) {

        return $this->driver->update($this->name, $fields, $criteria);

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

        if ($result = $this->execute())
            return $result->fetch(\PDO::FETCH_ASSOC, \PDO::FETCH_ORI_NEXT, $offset);

        return FALSE;

    }

    public function fetchAll() {

        if ($result = $this->execute())
            return $result->fetchAll(\PDO::FETCH_ASSOC);

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

}