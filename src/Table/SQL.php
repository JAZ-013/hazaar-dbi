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

        $symbols = array();

        while(preg_match('/(\((?:\(??[^\(]*?\)))+/', $line, $chunks)){

            $id = uniqid();

            $symbols[$id] = $this->parseCondition(trim($chunks[0], '()'));

            $line = str_replace($chunks[0], $id, $line);

        }

        $delimeters = array('and', 'or');

        $parts = preg_split('/\s*(' . implode('|', $delimeters) . ')\s*/i', $line, -1, PREG_SPLIT_DELIM_CAPTURE);

        if(count($parts) > 1){

            $conditions = array_combine($delimeters, array_fill(0, count($delimeters), array()));

            for($i=0; $i<count($parts); $i++){

                if(!($glu = strtolower(ake($parts, $i+1))))
                    $glu = strtolower(ake($parts, $i-1));

                $conditions['$' . $glu][] = array_unflatten($parts[$i]);

                $i++;

            }

        }else{

            $conditions = array_unflatten($parts[0]);

        }

        array_remove_empty($conditions);

        if(!count($symbols) > 0)
            return $conditions;

        $root = uniqid();

        $symbols[$root] = $conditions;

        foreach($symbols as $id => &$symbol){

            foreach($symbol as $glu => &$chunk){

                foreach($chunk as &$condition){

                    foreach($condition as $key => &$value){

                        if(substr($value, 0, 1) === "'" && substr($value, -1, 1) === "'"){
    
                            $value = substr($value, 1, -1);
    
                        }elseif(is_numeric($value)){
    
                            if(strpos($value, '.') === false)
                                $value = intval($value);
                            else
                                $value = floatval($value);
    
                        }elseif(is_boolean($value)){
    
                            $value = boolify($value);
    
                        }elseif(array_key_exists($value, $symbols)){
    
                            $condition[$key] =& $symbols[$value];
    
                        }

                    }

                }

            }

        }

        return $symbols[$root];

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

        $this->criteria = $this->parseCondition($line);

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
