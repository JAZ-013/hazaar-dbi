<?php

namespace Hazaar\Auth\Adapter;

class DBITable extends \Hazaar\Auth\Adapter implements _Interface {

    private $db;

    private $table;

    private $field_identity;

    private $field_credential;

    private $field_id;

    /*
     * Construct the new authentication object with the field names
     * in the model for id, user name, password and real name.
     */
    function __construct(\Hazaar\DBI\Adapter $db_adapter, $table = null, $field_identity = null, $field_credential = null, $field_id = 'id', $namespace = null, $cache_config = array(), $cache_backend = 'session') {

        $this->table = $table;

        $this->field_identity = $field_identity;

        $this->field_credential = $field_credential;

        $this->field_id = $field_id;

        $this->db = $db_adapter;

        parent::__construct($namespace, $cache_config, $cache_backend);

    }

    public function setTableName($table) {

        $this->table = $table;

    }

    public function setIdentityField($identity) {

        $this->field_identity = $identity;

    }

    public function setCredentialField($credential) {

        $this->field_credential = $credential;

    }

    /*
     * We override the authenticate method from base.  This uses the
     * required values from the parent object to get the credentials
     * and perform the authentication.
     */
    public function queryAuth($identity, $extra = array()) {

        if(!$this->field_identity)
            throw new \Exception('Identity field not set in ' . get_class($this) . '. This is required.');

        if(!$this->field_credential)
            throw new \Exception('Credential field not set in ' . get_class($this) . '. This is required.');

        $fields = array($this->field_id, $this->field_identity, $this->field_credential);

        if(is_array($extra)) $fields = array_merge($fields, $extra);

        $criteria = array($this->field_identity => $identity);

        if(!($record = $this->db->table($this->table)->findOne($criteria, $fields)))
            return false;

        $auth = array(
            'identity' => $record[$this->field_identity],
            'credential' => $record[$this->field_credential]
        );

        unset($record[$this->field_identity]);

        unset($record[$this->field_credential]);

        $auth['data'] = $record;

        return $auth;

    }

    public function addUser($identity, $credential) {

        $uid = $this->db->insert(array(
            $this->field_identity => $identity,
            $this->field_credential => $credential
        ));

        return $uid;

    }

    public function delUser($identity) {

        $this->db->delete(array($this->field_identity => $identity));

    }

}

