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

    public function fixValue($value){

        if(!$value)
            return $value;

        //Convert the 'now()' function call to the standard CURRENT_TIMESTAMP
        if(strtolower($value) == 'now()')
            return 'CURRENT_TIMESTAMP';

        //Strip any type casts
        if($pos = strpos($value, '::'))
            return substr($value, 0, $pos);

        return $value;

    }

    public function field($string) {

        //This matches an string that contain a non-word character, which means it is either a function call, concat or
        //at least definitely not a reserved word as all reserved words have only word characters
        if (preg_match('/\W/', $string))
            return $string;

        return '"' . $string . '"';

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

    public function listConstraints($table = NULL, $type = NULL, $invert_type = FALSE) {

        if(!$this->allow_constraints)
            return false;

        $constraints = array();

        $sql = "SELECT
                tc.constraint_name as name,
                tc.table_name as " . $this->field('table') . ",
                tc.table_schema as " . $this->field('schema') . ",
                kcu.column_name as " . $this->field('column') . ",
                ccu.table_schema AS foreign_schema,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column,
                tc.constraint_type as type,
                rc.match_option,
                rc.update_rule,
                rc.delete_rule
            FROM information_schema.table_constraints tc
            INNER JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_schema = tc.constraint_schema
                AND kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema = tc.table_schema
                AND kcu.table_name = tc.table_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            LEFT JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name
            WHERE tc.CONSTRAINT_SCHEMA='{$this->schema}'";

        if($table)
            $sql .= "\nAND tc.table_name='$table'";

        if ($type)
            $sql .= "\nAND tc.constraint_type" . ($invert_type ? '!=' : '=') . "'$type'";

        $sql .= ';';

        if ($result = $this->query($sql)) {

            while($row = $result->fetch(\PDO::FETCH_ASSOC)){

                $constraint = array(
                   'table' => $row['table'],
                   'column' => $row['column'],
                   'type' => $row['type']
                );

                if($row['type'] == 'FOREIGN KEY' && $row['foreign_table']){

                    $constraint['references'] = array(
                        'table' => $row['foreign_table'],
                        'column' => $row['foreign_column']
                    );

                }

                $constraints[$row['name']] = $constraint;

            }

            return $constraints;
        }

        return FALSE;

    }

    public function listIndexes($table = NULL){

        $sql = "SELECT s.nspname, t.relname as table_name, i.relname as index_name, array_to_string(array_agg(a.attname), ', ') as column_names, ix.indisunique
            FROM pg_namespace s, pg_class t, pg_class i, pg_index ix, pg_attribute a
            WHERE s.oid = t.relnamespace
                AND t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND s.nspname = 'public'";

        if($table)
            $sql .= "\nAND t.relname = '$table'";

        $sql .= "\nGROUP BY s.nspname, t.relname, i.relname, ix.indisunique ORDER BY t.relname, i.relname;";

        if(!($result = $this->query($sql)))
            throw new \Exception('Index list failed. ' . $this->errorInfo()[2]);

        $indexes = array();

        while($row = $result->fetch(\PDO::FETCH_ASSOC)){

            $indexes[$row['index_name']] = array(
                'columns' => array_map('trim', explode(',', $row['column_names'])),
                'unique' => boolify($row['indisunique'])
            );

        }

        return $indexes;

    }

    public function repair(){

        /*
         * Fix sequence current values to max value of column
         *
         * See: https://wiki.postgresql.org/wiki/Fixing_Sequences
         */
        $sql = "SELECT 'SELECT SETVAL(' ||
               quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
               ', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
               quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';'
               FROM pg_class AS S,
                    pg_depend AS D,
                    pg_class AS T,
                    pg_attribute AS C,
                    pg_tables AS PGT
               WHERE S.relkind = 'S'
                   AND S.oid = D.objid
                   AND D.refobjid = T.oid
                   AND D.refobjid = C.attrelid
                   AND D.refobjsubid = C.attnum
                   AND T.relname = PGT.tablename
               ORDER BY S.relname;";

        $result = $this->query($sql);

        while($seq = $result->fetchColumn(0))
            $this->query($seq);

        return true;

    }

}


