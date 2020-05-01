<?php

namespace Hazaar\DBI\Table;

class SQL extends \Hazaar\DBI\Table {

    private $keywords = array(
        'SELECT',
        'FROM',
        'WHERE', 
        'GROUP BY', 
        'HAVING', 
        'WINDOW',
        'UNION',
        'INTERSECT',
        'EXCEPT',
        'ORDER BY',
        'LIMIT',
        'OFFSET',
        'FETCH'
    );

    private $sql;

    function __construct(\Hazaar\DBI\Adapter $dbi, $sql){

        parent::__construct($dbi);

        $this->parse($sql);

    }

    public function parse($sql){

        if(strtoupper(substr(trim($sql), 0, 6)) !== 'SELECT')
            return false;

        $this->sql = $sql;

        $uSQL = strtoupper($sql);

        $loc = array();

        foreach($this->keywords as $keyword){

            if(($pos = strpos($uSQL, $keyword)) === false)
                continue;

            if(substr($uSQL, $pos - 1, 1) === '"')
                continue;

            $loc[$keyword] = $pos;

        }

        foreach($loc as $keyword => $pos){

            $next = strlen($sql);

            //Find the next position.  We intentionally do this instead of a sort so that SQL is processed in a known order.
            foreach(array_values($loc) as $value){

                if($value <= $pos || $value >= $next)
                    continue;

                $next = $value;

            }

            $line = trim(substr($sql, $pos + strlen($keyword), $next - ($pos + strlen($keyword))));

            $method = 'process' . str_replace(' ', '_', $keyword);

            if(method_exists($this, $method))
                call_user_func(array($this, $method), $line);

        }

        return true;

    }

    private function parseCondition($line){

        $delimeters = array('AND', 'OR');

        $parts = preg_split('/\s*(' . implode('|', $delimeters) . ')\s*/i', $line, -1, PREG_SPLIT_DELIM_CAPTURE);

        foreach($parts as $part){

            //if(in_array(\strtoupper($part), $delimeters))

        }

        //dump($parts);

        return array();

    }

    public function processSELECT($line){

        $this->fields = preg_split('/\s*,\s*/', $line);

    }

    public function processFROM($line){

        $parts = preg_split('/\s+/', $line, 2); 

        if(array_key_exists(0, $parts))
            $this->name = ake($parts, 0);

        if(array_key_exists(1, $parts))
            $this->alias = ake($parts, 1);

    }

    public function processWHERE($line){

        $this->where = $this->parseCondition($line);

    }

    public function processGROUP_BY($line){

        $this->group = preg_split('/\s*,\s*/', $line);

    }

    public function processHAVING($line){

        $this->having = $this->parseCondition($line);

    }

    public function processWINDOW($line){

        return null;

    }

    public function parseUNION($line){

        //TODO: Find all string between ( and )

        return null;

    }

    public function processINTERSECT($line){

        //TODO: Find all string between ( and )

        return null;

    }

    public function processEXCEPT($line){

        //TODO: Find all string between ( and )

        return null;

    }

    public function processORDER_BY($line){

        $parts = preg_split('/\s+/', $line, 2);

        if(array_key_exists(0, $parts)){

            $this->order = array(
                $parts[0] => (array_key_exists(1, $parts) && strtoupper(trim($parts[1])) === 'DESC' ? -1 : 1)
            );

        }

    }

    public function processLIMIT($line){

        $this->limit = strtoupper($line) === 'ALL' ? null : intval($line);
    
    }

    public function processOFFSET($line){

        $parts = preg_split('/\s+/', $line, 2);

        if(array_key_exists(0, $parts))
            $this->offset = intval($parts[0]);

    }

    public function processFETCH($line){

        $fetch_def = array();

        $parts = preg_split('/\s+/', $line);

        if(array_key_exists(0, $parts))
            $fetch_def['which'] = $parts[0];

        if(array_key_exists(1, $parts) && is_numeric($parts[1]))
            $fetch_def['count'] = intval($parts[1]);

        if(count($fetch_def) > 0)
            $this->fetch = $fetch_def;

    }

}
