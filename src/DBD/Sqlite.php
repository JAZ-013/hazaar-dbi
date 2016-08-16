<?php

namespace Hazaar\DBI\DBD;

class Sqlite extends BaseDriver {

    public $allow_constraints = false;

    public function connect($dsn, $username = null, $password = null, $driver_options = null) {

        $d_pos = strpos($dsn, ':');

        $driver = strtolower(substr($dsn, 0, $d_pos));

        if (!$driver == 'sqlite')
            return false;

        return parent::connect($dsn, $username, $password, $driver_options);

    }

    public function quote($string) {

        if ($string instanceof \Hazaar\Date)
            $string = $string->timestamp();

        if (!is_numeric($string))
            $string = $this->pdo->quote((string) $string);

        return $string;

    }

    public function listTables(){

        $sql = "SELECT tbl_name as name FROM sqlite_master WHERE type = 'table';";

        $result = $this->query($sql);

        return $result->fetchAll(\PDO::FETCH_ASSOC);

    }

    public function tableExists($table) {

        $info = new \Hazaar\DBI\Table($this, 'sqlite_master');

        return $info->exists(array(
            'name' => $table,
            'type' => 'table'
        ));

    }

}


