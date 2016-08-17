<?php

namespace Hazaar\DBI\DBD;

class Sqlite extends BaseDriver {


    public $allow_constraints = false;

    protected $reserved_words = array(
        'ABORT',
        'ACTION',
        'ADD',
        'AFTER',
        'ALL',
        'ALTER',
        'ANALYZE',
        'AND',
        'AS',
        'ASC',
        'ATTACH',
        'AUTOINCREMENT',
        'BEFORE',
        'BEGIN',
        'BETWEEN',
        'BY',
        'CASCADE',
        'CASE',
        'CAST',
        'CHECK',
        'COLLATE',
        'COLUMN',
        'COMMIT',
        'CONFLICT',
        'CONSTRAINT',
        'CREATE',
        'CROSS',
        'CURRENT_DATE',
        'CURRENT_TIME',
        'CURRENT_TIMESTAMP',
        'DATABASE',
        'DEFAULT',
        'DEFERRABLE',
        'DEFERRED',
        'DELETE',
        'DESC',
        'DETACH',
        'DISTINCT',
        'DROP',
        'EACH',
        'ELSE',
        'END',
        'ESCAPE',
        'EXCEPT',
        'EXCLUSIVE',
        'EXISTS',
        'EXPLAIN',
        'FAIL',
        'FOR',
        'FOREIGN',
        'FROM',
        'FULL',
        'GLOB',
        'GROUP',
        'HAVING',
        'IF',
        'IGNORE',
        'IMMEDIATE',
        'IN',
        'INDEX',
        'INDEXED',
        'INITIALLY',
        'INNER',
        'INSERT',
        'INSTEAD',
        'INTERSECT',
        'INTO',
        'IS',
        'ISNULL',
        'JOIN',
        'KEY',
        'LEFT',
        'LIKE',
        'LIMIT',
        'MATCH',
        'NATURAL',
        'NO',
        'NOT',
        'NOTNULL',
        'NULL',
        'OF',
        'OFFSET',
        'ON',
        'OR',
        'ORDER',
        'OUTER',
        'PLAN',
        'PRAGMA',
        'PRIMARY',
        'QUERY',
        'RAISE',
        'RECURSIVE',
        'REFERENCES',
        'REGEXP',
        'REINDEX',
        'RELEASE',
        'RENAME',
        'REPLACE',
        'RESTRICT',
        'RIGHT',
        'ROLLBACK',
        'ROW',
        'SAVEPOINT',
        'SELECT',
        'SET',
        'TABLE',
        'TEMP',
        'TEMPORARY',
        'THEN',
        'TO',
        'TRANSACTION',
        'TRIGGER',
        'UNION',
        'UNIQUE',
        'UPDATE',
        'USING',
        'VACUUM',
        'VALUES',
        'VIEW',
        'VIRTUAL',
        'WHEN',
        'WHERE',
        'WITH',
        'WITHOUT'
    );


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


