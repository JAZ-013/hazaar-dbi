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

    function __construct(\Hazaar\DBI\Adapter $adapter, $meta = array(), $data = array(), \PDOStatement $statement = null){

        $this->adapter = $adapter;

        $this->fields = $meta;

        $this->statement = $statement;

        $this->ignore_undefined = false;

        $this->allow_undefined = true;

        parent::__construct($data);

    }

    public function init(){

        foreach($this->fields as &$def){

            $def['changed'] = false;

            $def['update'] = array('post' => function($value, $key){
                $this->fields[$key]['changed'] = ($this->values[$key] !== $value);
            });

        }

        return $this->fields;

    }

    public function update(){

        if(!$this->statement instanceof \PDOStatement)
            throw new \Exception('Unable to perform updates without the original PDO statement!');

        $changes = array();

        foreach($this->fields as $key => $def){

            if($def['changed'] !== true)
                continue;

            if(!array_key_exists('table', $def))
                throw new \Exception('Unable to update ' . $key . ' with unknown table');

            $changes[$def['table']][] = $key . '=' . $this->adapter->prepareValue($this->get($key));

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

        if(!preg_match('/FROM\s+"?(\w+)"?(\s+"?(\w+)"?)?/', $this->statement->queryString, $matches))
            throw new \Exception('Can\'t figure out which table we\'re updating!');

        //Find the primary key for the primary table so we know which row we are updating
        foreach($this->adapter->listPrimaryKeys($matches[1]) as $data){

            if(!$this->has($data['column']))
                continue;

            $tables[$matches[1]] = array();

            if(isset($matches[3]) && !in_array(strtoupper($matches[3]), $keywords))
                $tables[$matches[1]]['alias'] = $matches[3];

            $tables[$matches[1]]['condition'] = ake($tables[$matches[1]], 'alias', $matches[1]) . '.' . $data['column'] . '=' . $this->get($data['column']);

            break;

        }

        if(!count($tables) > 0)
            throw new \Exception('Missing primary key in selection!');

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

        $this->adapter->beginTransaction();

        foreach($changes as $table => $updates){

            $sql = 'UPDATE ' . $table;

            if(array_key_exists('alias', $tables[$table]))
                $sql .= ' AS ' . $tables[$table]['alias'];

            $sql .= ' SET ' . implode(', ', $updates);

            $conditions = array();

            if($tables[$table]['condition']){

                $from = array();

                foreach($tables as $from_table => $data){

                    if($data['condition'])
                        $conditions[] = $data['condition'];

                    if($table !== $from_table)
                        $from[] = $from_table . (array_key_exists('alias', $data) ? ' ' . $data['alias'] : null);

                }

                $sql .= ' FROM ' . implode(', ', $from);

            }

            $sql .= ' WHERE ' . implode(' AND ', $conditions);

            if(!$this->adapter->query($sql)){

                $this->adapter->rollback();

                return false;

            }

        }

        $this->adapter->commit();

        return true;

    }

}
