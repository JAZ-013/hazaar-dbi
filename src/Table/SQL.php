<?php

namespace Hazaar\DBI\Table;

class SQL extends \Hazaar\DBI\Table {

    static private $keywords = array(
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

    private function splitWordBoundaries($string, $keywords, &$start_pos = null){

        $chunks = array();

        $start_pos = null;

        foreach($keywords as $keyword){

            if(preg_match_all("/\b$keyword\b/i", $string, $matches, PREG_OFFSET_CAPTURE) === 0)
                continue;

            foreach($matches[0] as $match){

                $pos = $match[1] + ((substr($match[0], 0, 1) === ' ') ? 1 : 0);

                if($start_pos === null)
                    $start_pos = $pos;

                if(!array_key_exists($keyword, $chunks))
                    $chunks[$keyword] = array();

                $chunks[$keyword][] = $pos;

            }

        }

        $result = array();

        foreach($chunks as $keyword => &$positions){

            foreach($positions as &$pos){

                $next = strlen($string);

                //Find the next position.  We intentionally do this instead of a sort so that SQL is processed in a known order.
                foreach(array_values($chunks) as $values){

                    foreach($values as $value){

                        if(is_int($value) && ($value <= $pos || $value >= $next))
                            continue;

                        $next = $value;

                    }

                }

                if(!array_key_exists($keyword, $result))
                    $result[$keyword] = array();

                $result[$keyword][] = trim(substr($string, $pos + strlen($keyword), $next - ($pos + strlen($keyword))));

            }

        }

        return $result;

    }

    public function parse($sql){

        if(strtoupper(substr(trim($sql), 0, 6)) !== 'SELECT')
            return false;

        $this->sql = $sql;

        $chunks = $this->splitWordBoundaries($sql, self::$keywords);

        foreach($chunks as $keyword => $values){

            foreach($values as $chunk){

                $method = 'process' . str_replace(' ', '_', $keyword);

                if(method_exists($this, $method))
                    call_user_func(array($this, $method), $chunk);

            }

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

        $parts = preg_split('/\b(' . implode('|', $delimeters) . ')\b/i', $line, -1, PREG_SPLIT_DELIM_CAPTURE);

        $conditions = array_combine($delimeters, array_fill(0, count($delimeters), array()));

        if(count($parts) > 1){

            for($i=0; $i<count($parts); $i++){

                if(!($glu = strtolower(ake($parts, $i+1))))
                    $glu = strtolower(ake($parts, $i-1));

                $conditions['$' . $glu][] = array_unflatten($parts[$i]);

                $i++;

            }

        }else{

            $conditions['$and'][] = array_unflatten($parts[0]);

        }

        array_remove_empty($conditions);

        if(!count($symbols) > 0)
            return $conditions;

        $root = uniqid();

        $symbols[$root] = $conditions;

        foreach($symbols as $id => &$symbol){

            foreach($symbol as $glu => &$chunk){

                if(!is_array($chunk)){

                    $glu = '$and';

                    $chunk = array($chunk);

                }

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

        $this->fields = array();

        $parts = preg_split('/\s*,\s*/', $line);

        foreach($parts as $part){

            if($chunks = $this->splitWordBoundaries($part, array('AS'), $pos))
                $this->fields[ake($chunks['AS'], 0)] = trim(substr($part, 0, $pos));
            else
                $this->fields[] = $part;

        }

    }

    public function processFROM($line){

        $keywords = array(
            'CROSS',
            'INNER',
            'LEFT',
            'RIGHT', 
            'FULL', 
            'OUTER', 
            'NATURAL',
            'JOIN'
        );

        if(!($chunks = $this->splitWordBoundaries($line, $keywords, $pos)))
            $pos = strlen($line);
        elseif(!$pos > 0)
            throw new \Exception('Parse error.  Got JOIN on missing table.');

        $parts = preg_split('/\s+/', trim(substr($line, 0, $pos)), 2); 

        if(array_key_exists(0, $parts))
            $this->name = ake($parts, 0);

        if(array_key_exists(1, $parts))
            $this->alias = ake($parts, 1);

        if(count($chunks) == 0)
            return;

        reset($chunks);

        while(key($chunks) !== null){

            $pos = null;

            $references = null;

            $alias = null;

            $type = key($chunks);

            if($type !== 'JOIN'){

                next($chunks);

                if(key($chunks) !== 'JOIN')
                    throw new \Exception('Parse error.  Expecting JOIN');

            }
            
            foreach(current($chunks) as $id => $join){

                $parts = $this->splitWordBoundaries($join, array('ON'), $pos);

                if(!$pos > 0)
                    throw new \Exception('Parse error.  Expecting join table name!');

                $join_parts = preg_split('/\s+/', trim(substr($join, 0, $pos)), 2); 

                $references = ake($join_parts, 0);

                $alias = ake($join_parts, 1, $references);

                $this->joins[$alias] = array(
                    'type' => $type,
                    'ref' => $references,
                    'on' => $this->parseCondition($parts['ON'][0]),
                    'alias' => $alias
                );

            }
            
            next($chunks);

        }

    }

    public function processWHERE($line){

        $line = trim($line);

        if(!(substr($line, 0, 1) === '(' && substr($line, -1, 1) === ')'))
            $line = '(' . $line . ')';

        $this->criteria = $this->parseCondition($line);

    }

    public function processGROUP_BY($line){

        $this->group = preg_split('/\s*,\s*/', $line);

    }

    public function processHAVING($line){

        $this->having = $this->parseCondition($line);

    }

    public function parseUNION($line){

        if(!array_key_exists('union', $this->subselects))
            $this->subselects['union'] = array();

        $this->subselects['union'][] = new SQL($line);

    }

    public function processINTERSECT($line){

        if(!array_key_exists('intersect', $this->subselects))
            $this->subselects['intersect'] = array();

        $this->subselects['intersect'][] = new SQL($line);

    }

    public function processEXCEPT($line){

        if(!array_key_exists('except', $this->subselects))
            $this->subselects['except'] = array();

        $this->subselects['except'][] = new SQL($line);

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
