<?php

namespace Hazaar\DBI\DBD;

class Pgsql extends BaseDriver {

    public function __construct($config){

        parent::__construct($config);

        $this->schema = 'public';

    }

    public function connects($dsn, $username = null, $password = null, $driver_options = null) {

        $d_pos = strpos($dsn, ':');

        $driver = strtolower(substr($dsn, 0, $d_pos));

        if (!$driver == 'pgsql')
            return false;

        $dsn_parts = array_unflatten(substr($dsn, $d_pos + 1));

        if (!array_key_exists('user', $dsn_parts))
            $dsn_parts['user'] = $username;

        if (!array_key_exists('password', $dsn_parts))
            $dsn_parts['password'] = $password;

        $dsn = $driver . ':' . array_flatten($dsn_parts);

        return parent::connect($dsn, null, null, $driver_options);

    }

    public function field($string) {

        if (strpos($string, '.') !== false || strpos($string, '(') !== false)
            return $string;

        return '"' . $string . '"';

    }

    public function createIndex($index_name, $idx_info) {

        if (!array_key_exists('table', $idx_info))
            return false;

        if (!array_key_exists('columns', $idx_info))
            return false;

        $sql = 'CREATE';

        if (array_key_exists('unique', $idx_info) && $idx_info['unique'])
            $sql .= ' UNIQUE';

        $sql .= ' INDEX ' . $index_name . ' ON ' . $idx_info['table'];

        if (array_key_exists('using', $idx_info) && $idx_info['using'])
            $sql .= ' USING ' . $idx_info['using'];

        $sql .= ' (' . implode(',', $idx_info['columns']) . ');';

        $affected = $this->exec($sql);

        if ($affected === false)
            return false;

        return true;

    }

    public function listTables() {

        $sql = "SELECT table_schema as \"schema\", table_name as name FROM information_schema.tables t WHERE table_type = 'BASE TABLE'";

        if($this->schema != 'public')
            $sql .= " AND table_schema = '$this->schema'";
        else
            $sql .= "AND table_schema NOT IN ( 'information_schema', 'pg_catalog' )";

        $sql .= " ORDER BY table_name DESC;";

        if ($result = $this->query($sql))
            return $result->fetchAll(\PDO::FETCH_ASSOC);

        return NULL;

    }
}


