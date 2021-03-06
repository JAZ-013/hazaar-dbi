<?php

/**
 * @file        Hazaar/DBI/Statement/Model.php
 *
 * @author      Jamie Carl <jamie@hazaarlabs.com>
 *
 * @copyright   Copyright (c) 2019 Jamie Carl (http://www.hazaarlabs.com)
 */

namespace Hazaar\DBI;

final class Row extends \Hazaar\Model\Strict {

    private $adapter;

    private $statement;

    function __construct(\Hazaar\DBI\Adapter $adapter, $meta = array(), $data = array(), \PDOStatement $statement = null, $options = array()){

        $this->adapter = $adapter;

        $this->fields = $meta;

        $this->statement = $statement;

        parent::__construct($data);

    }

    /**
     * Prepare the row values by checking for fields that are an array that should not be
     *
     * This will happen when a join selects multiple fields from different tables with the same name.  For example, when
     * doing a SELECT * with multiple tables that all have an 'id' column.  The 'id' columns from each table will clobber
     * the earlier value as each table is processed, meaning the Row::update() may not work.  To get around this, the
     * Row class is given data using the \PDO::FETCH_NAMED flag which will cause multiple columns with the same name to be
     * returned as an array.  However, the column will not have an array data type so we detect that and just grab the
     * first value in the array.
     *
     * @param mixed $data
     */
    public function prepare(&$data){

        foreach($this->fields as $key => $def){

            if(!(array_key_exists($key, $data) && is_array($data[$key])) || ake($def, 'type', 'none') === 'array')
                continue;

            $data[$key] = array_shift($data[$key]);

        }

    }

    public function init(){

        foreach($this->fields as &$def){

            $def = (array)$def;

            $def['changed'] = false;

            $def['update'] = array('post' => function($value, $key){
                $this->fields[$key]['changed'] = ($this->values[$key] !== $value);
            });

        }

        return $this->fields;

    }

    /**
     * Update the database with any changes to the current row, optionally providing updates directly
     * 
     * @param $data array Column updates that will be applied to the object directly
     */
    public function update($data = null){

        if(!$this->statement instanceof \PDOStatement)
            throw new \Hazaar\Exception('Unable to perform updates without the original PDO statement!');

        $schema = $this->adapter->getSchema();

        $changes = array();

        foreach($this->fields as $key => $def){

            if(is_array($data) && array_key_exists($key, $data)){

                $def['changed'] = true;

                $value = $data[$key];

            }else $value = $this->get($key);

            if(!(array_key_exists('changed', $def) && $def['changed'] === true))
                continue;

            if(!array_key_exists('table', $def))
                throw new \Hazaar\Exception('Unable to update ' . $key . ' with unknown table');

            $changes[ake($def, 'schema', $schema) . '.' . $def['table']][$key] = $value;

        }

        //Check if there are changes and if not, bomb out now as there's no point continuing.
        if(count($changes) <= 0)
            return false;

        //Defined keyword boundaries.  These are used to detect the end of things like table names if they have no alias.
        $keywords = array(
            'FROM',
            'INNER',
            'LEFT',
            'OUTER',
            'JOIN',
            'WHERE',
            'GROUP',
            'HAVING',
            'WINDOW',
            'UNION',
            'INTERSECT',
            'EXCEPT',
            'ORDER',
            'LIMIT',
            'OFFSET',
            'FETCH',
            'FOR'
        );

        $tables = array();

        if(!preg_match('/FROM\s+"?(\w+)"?(\."?(\w+)")?(\s+"?(\w+)"?)?/', $this->statement->queryString, $matches))
            throw new \Hazaar\Exception('Can\'t figure out which table we\'re updating!');

        $table_name = $matches[3] ? $matches[1] . '.' . $matches[3] : $matches[1];

        //Find the primary key for the primary table so we know which row we are updating
        foreach($this->adapter->listPrimaryKeys($table_name) as $data){

            if(!$this->has($data['column']))
                continue;

            $tables[$table_name] = array();

            if(isset($matches[5]) && !in_array(strtoupper($matches[5]), $keywords))
                $tables[$table_name]['alias'] = $matches[5];

            $tables[$table_name]['condition'] = ake($tables[$table_name], 'alias', $table_name) . '.' . $data['column'] . '=' . $this->get($data['column']);

            break;

        }

        if(!count($tables) > 0)
            throw new \Hazaar\Exception('Missing primary key in selection!');

        //Check and process joins
        if(preg_match_all('/JOIN\s+"?(\w+)"?(\s"?(\w+)"?)?\s+ON\s+("?[\w\.]+"?)\s?([\!=<>])\s?("?[\w\.]+"?)/i', $this->statement->queryString, $matches)){

            foreach($matches[0] as $idx => $match){

                $tables[$matches[1][$idx]] = array(
                    'condition' => $matches[4][$idx] . $matches[5][$idx] . $matches[6][$idx]
                );

                if($matches[3][$idx])
                    $tables[$matches[1][$idx]]['alias'] = $matches[3][$idx];

            }

        }

        $committed_updates = [];

        $this->adapter->beginTransaction();

        foreach($changes as $table => $updates){

            $conditions = array();

            $from = array();

            if($tables[$table]['condition']){

                foreach($tables as $from_table => $data){

                    if($data['condition'])
                        $conditions[] = $data['condition'];

                    if($table !== $from_table)
                        $from[] = $from_table . (array_key_exists('alias', $data) ? ' ' . $data['alias'] : null);

                }

            }

            if(array_key_exists('alias', $tables[$table]))
                $table = array($table, $tables[$table]['alias']);

            if(!($committed_updates[$table] = $this->adapter->update($table, $updates, $conditions, $from, array_keys($updates)))){

                $this->adapter->rollback();

                return false;

            }

        }

        if(count($committed_updates) === count($changes)){

            $this->adapter->commit();

            foreach($committed_updates as $table => $updates){

                foreach($updates as $update){

                    foreach($update as $key => $value)
                        $this->set($key, $update[$key]);

                }

            }

            return true;

        }

        $this->adapter->rollback();

        return false;

    }

    public function readBytea($value){

        if(!is_resource($value))
            return null;

        return stream_get_contents($value);

    }

}
