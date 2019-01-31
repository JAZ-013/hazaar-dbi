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

    private $table;

    function __construct(\Hazaar\DBI\Adapter $adapter, $meta, $data = array()){

        $this->adapter = $adapter;

        $this->table = $meta['table'];

        $this->fields = $meta['fields'];

        parent::__construct($data);

    }

    public function init(){

        foreach($this->fields as &$def){

            $def['changed'] = false;

            $def['update'] = array('post' => function($value, $key){
                $this->fields[$key]['changed'] = true;
            });

        }

        return $this->fields;

    }

    public function update(){

        if(!$this->table)
            return false;

        $changes = array();

        foreach($this->fields as $key => $def){

            if($def['changed'] !== true) continue;

            $changes[$key] = $this->get($key);

        }

        return $this->adapter->table($this->table)->update(array('id' => $this->id), $changes);

    }

}
