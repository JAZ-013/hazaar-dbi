<?php

namespace Hazaar\File\Backend;

class DBI implements _Interface {

    private $options;

    private $db;

    private $gridFS;

    private $collection;

    private $rootObject;

    public function __construct($options = array()) {

        $this->options = ($options instanceof \Hazaar\Map) ? $options : new \Hazaar\Map($options);

        $this->db = new \Hazaar\DBI\Adapter((($options->count() > 0) ? $options : array()));

        $this->loadRootObject();

    }

    public function refresh($reset = true) {

        return true;

    }

    public function loadRootObject() {

        if(! ($this->rootObject = $this->db->file->findOne(array('parents' => null)))) {

            $root = array(
                'kind'         => 'dir',
                'parents'      => null,
                'filename'     => 'ROOT',
                'created_on'   => new \Hazaar\Date(),
                'modified_on'  => null,
                'length'       => 0,
                'mime_type'    => 'directory'
            );

            if(!($id = $this->db->file->insert($root, 'id')))
                throw new \Exception('Unable to create DBI filesystem root object: ' . $this->db->errorInfo()[2]);

            $root['id'] = $id;

            /*
             * If we are recreating the ROOT document then everything is either
             *
             * a) New - In which case this won't do a thing
             *      - or possibly -
             * b) Screwed - In which case this should make everything work again.
             *
             */
            $this->db->file->update(array('parents' => array('$not' => null)), array('parents' => array('$array' => $root['id'])));

            $this->rootObject = $root;

        }

        if(!$this->rootObject['created_on'] instanceof \Hazaar\Date)
            $this->rootObject['created_on'] = new \Hazaar\Date($this->rootObject['created_on']);

        if($this->rootObject['modified_on'] && !$this->rootObject['modified_on'] instanceof \Hazaar\Date)
            $this->rootObject['modified_on'] = new \Hazaar\Date($this->rootObject['modified_on']);

        return is_array($this->rootObject);

    }

    private function loadObjects(&$parent = null) {

        if(! is_array($parent))
            return false;

        $q = $this->db->query('SELECT * FROM "file" WHERE filename IS NOT NULL AND ' . $parent['id'] . ' = ANY(parents);');

        $parent['items'] = array();

        while($object = $q->row())
            $parent['items'][$object['filename']] = $object;

        return true;

    }

    private function & info($path) {

        $parent =& $this->rootObject;

        if($path === '/')
            return $parent;

        $parts = explode('/', $path);

        /*
         * Paths have a forward slash on the start so we need to drop the first element.
         */
        array_shift($parts);

        $false = false;

        foreach($parts as $part) {

            if($part === '')
                continue;

            if(! (array_key_exists('items', $parent) && is_array($parent['items'])))
                $this->loadObjects($parent);

            if(! array_key_exists($part, $parent['items']))
                return $false;

            $parent =& $parent['items'][$part];

            if(! $parent)
                return $false;

        }

        return $parent;

    }

    public function fsck() {

        $c = $this->db->file->find(array(), array('filename', 'parents'));

        while($file = $c->row()) {

            $update = array();

            if(! is_array($file['parents']))
                continue;

            /*
             * Make sure an objects parents exist!
             *
             * NOTE: This is allowed to be slow as it is never usually executed.
             */
            foreach($file['parents'] as $index => $parentID) {

                $parent = $this->db->file->findOne(array('id' => $parentID));

                if(! $parent)
                    $update[] = $index;

            }

            if(count($update) > 0) {

                foreach($update as $index)
                    unset($file['parents'][$index]);

                /*
                 * Fix up any parentless objects
                 */
                if(count($file['parents']) == 0)
                    $file['parents'] = array($this->rootObject['_id']);

                $this->db->file->update(array('id' => $file['id']), array('parents' => array('$array' => $file['parents'])));

            }

        }

        $this->loadRootObject();

        return true;

    }

    public function scandir($path, $regex_filter = null, $show_hidden = false) {

        if(! ($parent = $this->info($path)))
            return false;

        if(! array_key_exists('items', $parent))
            $this->loadObjects($parent);

        $list = array();

        foreach($parent['items'] as $filename => $file) {

            $fullpath = $path . $file['filename'];

            if($regex_filter && ! preg_match($regex_filter, $fullpath))
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
    public function filemtime($path) {

        if($info = $this->info($path))
            return strtotime(ake($info, 'modified_on', $info['created_on'], true));

        return false;

    }

    public function filesize($path) {

        if(! ($info = $this->info($path)))
            return false;

        return ake($info, 'length', 0);

    }

    public function fileperms($path) {

        if(! ($info = $this->info($path)))
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

        $parent =& $this->info(dirname($path));

        $info = array(
            'kind'         => 'dir',
            'parents'      => array('$array' => $parent['id']),
            'filename'     => basename($path),
            'length'       => 0,
            'created_on'   => new \Hazaar\Date(),
            'modified_on'  => null
        );

        if(!($id = $this->db->file->insert($info, 'id')) > 0)
            return false;

        $info['id'] = $id;

        if(! array_key_exists('items', $parent))
            $parent['items'] = array();

        $parent['items'][$info['filename']] = $info;

        return true;

    }

    public function unlink($path) {

        if($info = $this->info($path)) {

            $parent =& $this->info(dirname($path));

            if(count($info['parents']) > 1){

                if(($key = array_search($parent['id'], $info['parents'])) !== null){

                    unset($info['parents'][$key]);

                    $this->db->file->update(array('id' => $info['id']), array('parents' => array('$array' => $info['parents'])));

                }

            }else{

                if($info['kind'] != 'dir')
                    $this->db->file_chunk->delete(array('file_id' => $info['id']));

                if($this->db->file->delete(array('id' => $info['id']))){

                    unset($parent['items'][$info['filename']]);

                    return true;

                }

            }

        }

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

                        $fullPath = $path . '/' . $file;

                        if($this->is_dir($fullPath))
                            $this->rmdir($fullPath, true);

                        else
                            $this->unlink($fullPath);

                    }

                } else {

                    return false;

                }

            }

            if($path == '/')
                return true;

            return $this->unlink($path);

        }

        return false;

    }

    public function read($path) {

        if(! ($item = $this->info($path)))
            return false;

        if(! ($file = $this->db->file_chunk->findOne(array('file_id' => $item['id']))))
            return false;

        return stream_get_contents($file['data']);

    }

    public function write($path, $bytes, $content_type, $overwrite = false) {

        $parent =& $this->info(dirname($path));

        if(! $parent)
            return false;

        $md5 = md5($bytes);

        if($info = $this->db->file->findOne(array('md5' => $md5))) {

            if(in_array($parent['id'], $info['parents']))
                return false;

            $data = array(
                'modified_on' => new \Hazaar\Date(),
                'parents' => array('$push' => $parent['id'])
            );

            if($this->db->file->update(array('id' => $info['id']), $data)){

                if(! array_key_exists('items', $parent))
                    $parent['items'] = array();

                $parent['items'][$info['filename']] = $info;

                return true;

            }

        } else {

            $size = strlen($bytes);

            $fileInfo = array(
                'kind'         => 'file',
                'parents'      => array('$array' => $parent['id']),
                'filename'     => basename($path),
                'created_on'   => new \Hazaar\Date(),
                'modified_on'  => null,
                'length'       => $size,
                'mime_type'    => $content_type,
                'md5'          => $md5
            );

            if($id = $this->db->file->insert($fileInfo, 'id')) {

                $stmt = $this->db->prepare('INSERT INTO file_chunk (file_id, n, data) VALUES (?, ?, ?);');

                $stmt->bindParam(1, $id);

                $n = 0;

                $stmt->bindParam(2, $n); //Support for multiple chunks will come later at some point

                $stmt->bindParam(3, $bytes, \PDO::PARAM_LOB);

                //Insert the data chunk.  If this fails then remote the file.  We do this without a transaction due to the potential size.
                if(!$stmt->execute()){

                    $this->db->file->delete(array('id' => $id));

                    throw new \Exception($this->db->errorInfo()[2]);

                }

                $fileInfo['id'] = $id;

                if(! array_key_exists('items', $parent))
                    $parent['items'] = array();

                $parent['items'][$fileInfo['filename']] = $fileInfo;

                return true;

            }

        }

        return false;

    }

    public function upload($path, $file, $overwrite = false) {

        return $this->write(rtrim($path, '/') . '/' . $file['name'], file_get_contents($file['tmp_name']), $file['type'], $overwrite);

    }

    public function copy($src, $dst, $recursive = false) {

        if(! ($source = $this->info($src)))
            return false;

        $dstParent =& $this->info($dst);

        if($dstParent) {

            if($dstParent['kind'] !== 'dir')
                return false;

        } else {

            $dstParent =& $this->info(dirname($dst));

        }

        if(! $dstParent)
            return false;

        $data = array(
            'modified_on' => new \Hazaar\Date()
        );

        if(!in_array($dstParent['id'], $source['parents']))
            $data['parents'] = array('$push' => $dstParent['id']);

        if(!$this->db->file->update(array('id' => $source['id']), $data))
            return false;

        if(! array_key_exists('items', $dstParent))
            $dstParent['items'] = array();

        $dstParent['items'][$source['filename']] = $source;

        return true;

    }

    public function link($src, $dst) {

        if(! ($source = $this->info($src)))
            return false;

        $dstParent =& $this->info($dst);

        if($dstParent) {

            if($dstParent['kind'] !== 'dir')
                return false;

        } else {

            $dstParent =& $this->info(dirname($dst));

        }

        if(! $dstParent)
            return false;

        $data = array(
            'modified_on' => new \MongoDate
        );

        if(! in_array($dstParent['id'], $source['parents']))
            $data['parents'] = array('$push' => $dstParent['id']);

        if(!$this->db->file->update(array('id' => $source['id']), $data))
            return false;

        if(! array_key_exists('items', $dstParent))
            $dstParent['items'] = array();

        $dstParent['items'][$source['filename']] = $source;

        return true;

    }

    public function move($src, $dst) {

        if(substr($dst, 0, strlen($src)) == $src)
            return false;

        if(! ($source = $this->info($src)))
            return false;

        $srcParent =& $this->info(dirname($src));

        $data = array(
            'modified_on' => new \Hazaar\Date()
        );

        $dstParent =& $this->info($dst);

        if($dstParent) {

            //If the destination exists and is NOT a directory, return false so we don't overwrite an existing file.
            if($dstParent['kind'] !== 'dir')
                return false;

            //Double check the source parent exists and remove it from the parents array
            if(($key = array_search($srcParent['id'], $source['parents'])) !== null)
                unset($source['parents'][$key]);

            if(!in_array($dstParent['id'], $source['parents'])){

                array_push($source['parents'], $dstParent['id']);

                $data['parents'] = array('$array' => $source['parents']);

            }

        } else {

            //We are renaming the file.
            if($source['filename'] != basename($dst))
                $dstParent['filename'] = $data['filename'] = basename($dst);

            $dstParent =& $this->info(dirname($dst));

            //Update the parents items array key with the new name.
            $basename = basename($src);

            $dstParent['items'][basename($dst)] = $dstParent['items'][$basename];

            unset($dstParent['items'][$basename]);

        }

        if(!$this->db->file->update(array('id' => $source['id']), $data))
            throw new \Exception($this->db->errorInfo()[2]);

        return true;

    }

    public function chmod($path, $mode) {

        if(! is_int($mode))
            return false;

        if($target =& $this->info($path)) {

            $target['mode'] = $mode;

            return $this->db->file->update(array('id' => $target['id']), array('mode' => $mode));

        }

        return false;

    }

    public function chown($path, $user) {

        if($target =& $this->info($path)) {

            $target['owner'] = $user;

            return $this->db->file->update(array('id' => $target['id']), array('owner' => $user));

        }

        return false;

    }

    public function chgrp($path, $group) {

        if($target =& $this->info($path)) {

            $target['group'] = $group;

            return $this->collection->update(array('id' => $target['id']), array('group' => $group));

        }

        return false;

    }

    public function set_meta($path, $values) {

        if($target =& $this->info($path))
            return $this->db->file->update(array('id' => $target['id']), array('metadata' => json_encode($values)));

        if($parent =& $this->info(dirname($path))) {

            $parent['items'][basename($path)]['meta'] = $values;

            return true;

        }

        return false;

    }

    public function get_meta($path, $key = null) {

        if(! ($info = $this->info($path)))
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

}
