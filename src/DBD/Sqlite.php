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

    static function mkdsn($config){

        $filename = ($config->has('filename') ? $config->filename : 'database.sqlite' );

        return 'sqlite:' . \Hazaar\Application::getInstance()->runtimePath($filename);

    }

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

        $tables = array();

        $sql = "SELECT tbl_name as name FROM sqlite_master WHERE type = 'table';";

        $result = $this->query($sql);

        while($table = $result->fetch(\PDO::FETCH_ASSOC)){

            //Ignore internal SQLite tables.
            if(substr($table['name'], 0, 7) == 'sqlite_')
                continue;

            $tables[] = array('name' => $table['name']);

        }

        return $tables;

    }

    public function tableExists($table) {

        $info = new \Hazaar\DBI\Table($this, 'sqlite_master');

        return $info->exists(array(
            'name' => $table,
            'type' => 'table'
        ));

    }

    public function describeTable($name, $sort = NULL){

        $columns = array();

        $name = $this->tableName($name);

        $sql = "PRAGMA table_info('$name');";

        $result = $this->query($sql);

        $ordinal_position = 0;

        while($col = $result->fetch(\PDO::FETCH_ASSOC)) {

            //SQLite does not have ordinal position so we generate it
            $ordinal_position++;

            $columns[] = array(
                'name' => $col['name'],
                'ordinal_position' => $ordinal_position,
                'default' => $col['dflt_value'],
                'not_null' => boolify($col['notnull']),
                'data_type' => $this->type($col['type']),
                'length' => null,
                'primarykey' => boolify($col['pk'])
            );

        }

        return $columns;
    }

    public function prepareValue($value) {

        if (is_bool($value))
            $value = ($value ? 1 : 0);

        return parent::prepareValue($value);

    }

    public function tableName($name){

        $parts = explode('.', $name);

        if(count($parts) > 1)
            $name = $parts[1];

        return $name;

    }

}


