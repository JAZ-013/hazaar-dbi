<?php

namespace Hazaar\File\Backend;

class DBI implements _Interface {

    public  $separator = '/';

    private $options;

    private $db;

    private $rootObject;

    public function __construct($options = array()) {

        $defaults = array(
            'dbi' => \Hazaar\DBI\Adapter::getDefaultConfig(),
            'initialise' => true,
            'chunkSize' => 4194304
        );

        if($options instanceof \Hazaar\Map)
            $options->enhance($defaults);
        else
            $options = new \Hazaar\Map($defaults, $options);

        $this->options = $options;

        if(is_string($this->options['chunkSize']))
            $this->options['chunkSize'] = intval(bytes_str($this->options['chunkSize']));

        $this->db = new \Hazaar\DBI\Adapter($this->options['dbi']);

        $this->loadRootObject();

    }

    public function refresh($reset = true) {

        return true;

    }

    public function loadRootObject() {

        if(!($this->rootObject = $this->db->hz_file->findOne(array('parent' => null)))) {

            $this->rootObject = array(
                'kind'         => 'dir',
                'parent'       => null,
                'filename'     => 'ROOT',
                'created_on'   => new \Hazaar\Date(),
                'modified_on'  => null,
                'length'       => 0,
                'mime_type'    => 'directory'
            );

            if(!($this->rootObject['id'] = $this->db->hz_file->insert($this->rootObject, 'id')))
                throw new \Exception('Unable to create DBI filesystem root object: ' . $this->db->errorInfo()[2]);

            /*
             * If we are recreating the ROOT document then everything is either
             *
             * a) New - In which case this won't do a thing
             *      - or possibly -
             * b) Screwed - In which case this should make everything work again.
             *
             */
            $this->fsck(true);

        }

        if(!$this->rootObject['created_on'] instanceof \Hazaar\Date)
            $this->rootObject['created_on'] = new \Hazaar\Date($this->rootObject['created_on']);

        if($this->rootObject['modified_on'] && !$this->rootObject['modified_on'] instanceof \Hazaar\Date)
            $this->rootObject['modified_on'] = new \Hazaar\Date($this->rootObject['modified_on']);

        return is_array($this->rootObject);

    }

    private function loadObjects(&$parent = null) {

        if(!is_array($parent))
            return false;

        $q = $this->db->hz_file->find(array('parent' => $parent['id']));

        $parent['items'] = array();

        while($object = $q->fetch())
            $parent['items'][$object['filename']] = $object;

        return true;

    }

    private function dirname($path){

        if(($pos = strrpos($path, $this->separator)) !== false)
            $path = substr($path, 0, (($pos === 0) ? $pos + 1 : $pos));

        return $path;

    }

    private function & info($path) {

        $parent =& $this->rootObject;

        if($path === $this->separator)
            return $parent;

        $parts = explode($this->separator, $path);

        $false = false;

        foreach($parts as $part) {

            if($part === '')
                continue;

            if(!(array_key_exists('items', $parent) && is_array($parent['items'])))
                $this->loadObjects($parent);

            if(!array_key_exists($part, $parent['items']))
                return $false;

            $parent =& $parent['items'][$part];

            if(!$parent)
                return $false;

        }

        return $parent;

    }

    public function fsck($skip_root_reload = false) {

        $c = $this->db->hz_file->find(array(), array('id', 'filename', 'parent'));

        while($file = $c->fetch()) {

            if(!$file['parent'])
                continue;

            /*
             * Make sure an objects parent exist!
             */
            if(!$this->db->hz_file->exists(array('id' => $file['parent'])))
                $this->db->hz_file->update(array('id' => $file['id']), array('parent' => $this->rootObject['id']));

        }

        //Remove headless chunks
        $select = $this->db->hz_file_chunk('fc')
            ->leftjoin('hz_file', array('f.start_chunk' => array('$ref' => 'fc.id')), 'f')
            ->find(array('f.id' => null, 'fc.parent' => null), 'fc.id');

        while($row = $select->fetch())
            $this->clean_chunk($row['id']);

        if($skip_root_reload !== true)
            $this->loadRootObject();

        $this->db->repair();

        return true;

    }

    public function scandir($path, $regex_filter = null, $show_hidden = false) {

        if(!($parent =& $this->info($path)))
            return false;

        if(!array_key_exists('items', $parent))
            $this->loadObjects($parent);

        $list = array();

        foreach($parent['items'] as $file) {

            $fullpath = $path . $file['filename'];

            if($regex_filter && !preg_match($regex_filter, $fullpath))
                continue;

            $list[] = $file['filename'];

        }

        return $list;

    }

    //Check if file/path exists
    public function exists($path) {

        return is_array($this->info($path));

    }

    public function realpath($path) {

        return $path;

    }

    public function is_readable($path) {

        return true;

    }

    public function is_writable($path) {

        return true;

    }

    //true if path is a directory
    public function is_dir($path) {

        if($info = $this->info($path))
            return (ake($info, 'kind') == 'dir');

        return false;

    }

    //true if path is a symlink
    public function is_link($path) {

        return false;

    }

    //true if path is a normal file
    public function is_file($path) {

        if($info = $this->info($path))
            return (ake($info, 'kind', 'file') == 'file');

        return false;

    }

    //Returns the file type
    public function filetype($path) {

        if($info = $this->info($path))
            return ake($info, 'kind', 'file');

        return false;

    }

    //Returns the file modification time
    public function filectime($path) {

        if($info = $this->info($path))
            return strtotime(ake($info, 'created_on'));

        return false;

    }

    //Returns the file modification time
    public function filemtime($path) {

        if($info = $this->info($path))
            return strtotime(ake($info, 'modified_on', $info['created_on'], true));

        return false;

    }

    //Returns the file modification time
    public function fileatime($path) {

        return false;

    }

    public function filesize($path) {

        if(!($info = $this->info($path)))
            return false;

        return ake($info, 'length', 0);

    }

    public function fileperms($path) {

        if(!($info = $this->info($path)))
            return false;

        return ake($info, 'mode');

    }

    public function mime_content_type($path) {

        if($info = $this->info($path))
            return ake($info, 'mime_type', false);

        return false;

    }

    public function md5Checksum($path) {

        if($info = $this->info($path))
            return ake($info, 'md5');

        return false;

    }

    public function thumbnail($path, $params = array()) {

        return false;

    }

    public function mkdir($path) {

        if($info = $this->info($path))
            return false;

        if(!($parent =& $this->info($this->dirname($path))))
            throw new \Exception('Unable to determine parent of path: ' . $path);

        $info = array(
            'kind'         => 'dir',
            'parent'       => $parent['id'],
            'filename'     => basename($path),
            'length'       => 0,
            'created_on'   => new \Hazaar\Date(),
            'modified_on'  => null
        );

        if(!($id = $this->db->hz_file->insert($info, 'id')) > 0)
            return false;

        $info['id'] = $id;

        if(!array_key_exists('items', $parent))
            $parent['items'] = array();

        $parent['items'][$info['filename']] = $info;

        return true;

    }

    public function unlink($path) {

        if(!($info = $this->info($path)))
            return false;

        if(!($parent =& $this->info($this->dirname($path))))
            throw new \Exception('Unable to determine parent of path: ' . $path);

        if(!$this->db->hz_file->delete(array('id' => $info['id'])))
            return false;

        unset($parent['items'][$info['filename']]);

        if($info['kind'] !== 'dir')
            $this->clean_chunk($info['start_chunk']);

        return true;

    }

    public function rmdir($path, $recurse = false) {

        if($info = $this->info($path)) {

            if($info['kind'] != 'dir')
                return false;

            $dir = $this->scandir($path, null, true);

            if(count($dir) > 0) {

                if($recurse) {

                    foreach($dir as $file) {

                        $fullPath = $path . $this->separator . $file;

                        if($this->is_dir($fullPath))
                            $this->rmdir($fullPath, true);

                        else
                            $this->unlink($fullPath);

                    }

                } else {

                    return false;

                }

            }

            if($path == $this->separator)
                return true;

            return $this->unlink($path);

        }

        return false;

    }

    public function read($path) {

        if(!($item = $this->info($path)))
            return false;

        $sql = 'WITH RECURSIVE chunk_chain(id, parent, data) AS (SELECT id, parent, data FROM hz_file_chunk WHERE id = ' . $item['start_chunk'];

        $sql .= ' UNION ALL SELECT fc.id, fc.parent, fc.data FROM chunk_chain cc INNER JOIN hz_file_chunk AS fc ON fc.parent = cc.id)';

        $sql .= ' SELECT data FROM chunk_chain;';

        $result = $this->db->query($sql);

        $bytes = '';

        while($chunk = $result->fetch())
            $bytes .= stream_get_contents($chunk['data']);

        return $bytes;

    }

    public function write($path, $bytes, $content_type, $overwrite = false) {

        if(!($parent =& $this->info($this->dirname($path))))
            throw new \Exception('Unable to determine parent of path: ' . $path);

        if(!$parent)
            return false;

        $size = strlen($bytes);

        $md5 = md5($bytes);

        if($info = $this->db->hz_file->findOne(array('md5' => $md5))) {

            $chunk_id = $info['start_chunk'];

        } else {

            $chunk_size = $this->options['chunkSize'];

            $stmt = $this->db->prepare('INSERT INTO hz_file_chunk (parent, n, data) VALUES (?, ?, ?) RETURNING id;');

            $chunks = intval(ceil($size / $chunk_size));

            $last_chunk_id = null;

            for($n = 0; $n < $chunks; $n++){

                $stmt->bindParam(1, $last_chunk_id);

                $stmt->bindParam(2, $n); //Support for multiple chunks will come later at some point

                if($size > $chunk_size){

                    $chunk = substr($bytes, $n * $chunk_size, $chunk_size);

                    $stmt->bindParam(3, $chunk, \PDO::PARAM_LOB);

                }else{

                    $stmt->bindParam(3, $bytes, \PDO::PARAM_LOB);

                }

                if(!($last_chunk_id = $stmt->execute()) > 0)
                    throw $this->db->errorException('Write failed!');

                settype($last_chunk_id, 'integer');

                if(intval($n) === 0)
                    $chunk_id = $last_chunk_id;

            }

        }

        if($fileInfo =& $this->info($path)){

            //If it's the same chunk, just bomb out because we are not updating anything
            if(($old_chunk = $fileInfo['start_chunk']) === $chunk_id)
                return false;

            $data = array(
                'start_chunk' => $fileInfo['start_chunk'] = $chunk_id,
                'md5'         => $fileInfo['md5']         = $md5,
                'modified_on' => $fileInfo['modified_on'] = new \Hazaar\Date
            );

            if(!$this->db->hz_file->update(array('id' => $fileInfo['id']), $data))
                return false;

            $this->clean_chunk($old_chunk);

        }else{

            $fileInfo = array(
                'kind'         => 'file',
                'parent'       => $parent['id'],
                'start_chunk'  => $chunk_id,
                'filename'     => basename($path),
                'created_on'   => new \Hazaar\Date(),
                'modified_on'  => new \Hazaar\Date(),
                'length'       => $size,
                'mime_type'    => $content_type,
                'md5'          => $md5
            );

            if(!($id = $this->db->hz_file->insert($fileInfo, 'id')))
                throw $this->db->errorException();

            $fileInfo['id'] = $id;

            if(!array_key_exists('items', $parent))
                $parent['items'] = array();

            $parent['items'][$fileInfo['filename']] = $fileInfo;

        }

        return true;

    }

    private function clean_chunk($start_chunk_id){

        if($this->db->hz_file->find(array('start_chunk' => $start_chunk_id))->count() !== 0)
            return false;

        $sql = 'WITH RECURSIVE chunk_chain(id, parent) AS (SELECT id, parent FROM hz_file_chunk WHERE id = ' . $start_chunk_id;

        $sql .= ' UNION ALL SELECT fc.id, fc.parent FROM chunk_chain cc INNER JOIN hz_file_chunk AS fc ON fc.parent = cc.id)';

        $sql .= ' SELECT id FROM chunk_chain;';

        if(!($result = $this->db->query($sql)))
            throw new \Exception($this->db->errorInfo()[2]);

        return $this->db->hz_file_chunk->delete(array('id' => array('$in' => array_column($result->fetchAll(), 'id'))));

    }

    public function upload($path, $file, $overwrite = false) {

        return $this->write(rtrim($path, $this->separator) . $this->separator . $file['name'], file_get_contents($file['tmp_name']), $file['type'], $overwrite);

    }

    public function copy($src, $dst, $recursive = false) {

        if(!($source = $this->info($src)))
            return false;

        if(!($dstParent =& $this->info($this->dirname($dst))))
            throw new \Exception('Unable to determine parent of path: ' . $dst);

        if($dstParent) {

            if($dstParent['kind'] !== 'dir')
                return false;

        } else {

            if(!($dstParent =& $this->info($this->dirname($dst))))
                throw new \Exception('Unable to determine parent of path: ' . $dst);

        }

        if(!$dstParent)
            return false;

        $target = $source;

        unset($target['id']);

        $target['modified_on'] = new \Hazaar\Date();

        $target['parent'] = $dstParent['id'];

        if(!$this->db->hz_file->insert($target))
            return false;

        if(!array_key_exists('items', $dstParent))
            $dstParent['items'] = array();

        $dstParent['items'][$target['filename']] = $target;

        return true;

    }

    public function link($src, $dst) {

        if(!($source = $this->info($src)))
            return false;

        if(!($dstParent =& $this->info($this->dirname($dst))))
            throw new \Exception('Unable to determine parent of path: ' . $dst);

        if($dstParent) {

            if($dstParent['kind'] !== 'dir')
                return false;

        } else {

            if(!($dstParent =& $this->info($this->dirname($dst))))
                throw new \Exception('Unable to determine parent of path: ' . $dst);

        }

        if(!$dstParent)
            return false;

        $data = array(
            'modified_on' => new \MongoDate,
            'parent' => $dstParent['id']
        );

        if(!$this->db->hz_file->update(array('id' => $source['id']), $data))
            return false;

        if(!array_key_exists('items', $dstParent))
            $dstParent['items'] = array();

        $dstParent['items'][$source['filename']] = $source;

        return true;

    }

    public function move($src, $dst) {

        if(substr($dst, 0, strlen($src)) == $src)
            return false;

        if(!($source = $this->info($src)))
            return false;

        if(!($srcParent =& $this->info($this->dirname($src))))
            throw new \Exception('Unable to determine parent of path: ' . $src);

        $data = array(
            'modified_on' => new \Hazaar\Date()
        );

        if(!($dstParent =& $this->info($this->dirname($dst))))
            throw new \Exception('Unable to determine parent of path: ' . $dst);

        if($srcParent['id'] === $dstParent['id']) { //We are renaming the file.

            $data['filename'] = basename($dst);

            //Update the parents items array key with the new name.
            $basename = basename($src);

            $dstParent['items'][$data['filename']] = $dstParent['items'][$basename];

            unset($dstParent['items'][$basename]);

        }else{

            //If the destination exists and is NOT a directory, return false so we don't overwrite an existing file.
            if($dstParent['kind'] !== 'dir')
                return false;

            $data['parent'] = $dstParent['id'];

        }

        if(!$this->db->hz_file->update(array('id' => $source['id']), $data))
            throw new \Exception($this->db->errorInfo()[2]);

        return true;

    }

    public function chmod($path, $mode) {

        if(!is_int($mode))
            return false;

        if($target =& $this->info($path)) {

            $target['mode'] = $mode;

            return $this->db->hz_file->update(array('id' => $target['id']), array('mode' => $mode));

        }

        return false;

    }

    public function chown($path, $user) {

        if($target =& $this->info($path)) {

            $target['owner'] = $user;

            return $this->db->hz_file->update(array('id' => $target['id']), array('owner' => $user));

        }

        return false;

    }

    public function chgrp($path, $group) {

        if($target =& $this->info($path)) {

            $target['group'] = $group;

            return $this->db->hz_file->update(array('id' => $target['id']), array('group' => $group));

        }

        return false;

    }

    public function set_meta($path, $values) {

        if($target =& $this->info($path))
            return $this->db->hz_file->update(array('id' => $target['id']), array('metadata' => json_encode($values)));

        if($parent =& $this->info($this->dirname($path))) {

            $parent['items'][basename($path)]['meta'] = $values;

            return true;

        }

        return false;

    }

    public function get_meta($path, $key = null) {

        if(!($info = $this->info($path)))
            return false;

        if(array_key_exists('metadata', $info)) {

            if($key)
                return ake($info['metadata'], $key);

            return $info['metadata'];

        }

        return null;

    }

    public function preview_uri($path) {

        return false;

    }

    public function direct_uri($path) {

        return false;

    }

    public function find($search = NULL, $path = '/', $case_insensitive = false){

        $list = array();

        $method = ($case_insensitive ? '$ilike' : '$like');

        $result = $this->db->file->find(array(array('filename' => array($method => str_replace('*', '%', $search)))));

        while($file = $result->fetch()){

            $list[] = '/' . $file['filename'];

        }

        return $list;

    }

}
