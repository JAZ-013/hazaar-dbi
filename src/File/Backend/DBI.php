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

        if(! ($this->rootObject = $this->db->file->findOne(array('parent' => null)))) {

            $root = array(
                'kind'         => 'dir',
                'parent'       => null,
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
            $this->db->file->update(array('parent' => array('$not' => null)), array('parent' => $root['id']));

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

        $criteria = array(
            array('filename' => array('$ne' => null)),
            'parent' => $parent['id']
        );

        $q = $this->db->file->find($criteria);

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

        die(__METHOD__);

        $c = $this->collection->find(array(), array('filename' => true, 'parents' => true));

        while($file = $c->getNext()) {

            $update = array();

            if(! is_array($file['parents']))
                continue;

            /*
             * Make sure an objects parents exist!
             *
             * NOTE: This is allowed to be slow as it is never usually executed.
             */
            foreach($file['parents'] as $index => $parentID) {

                $parent = $this->collection->findOne(array('_id' => $parentID));

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

                $this->collection->update(array('_id' => $file['_id']), array('$set' => array('parents' => $file['parents'])));

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
            'parent'       => $parent['id'],
            'filename'     => basename($path),
            'length'       => 0,
            'created_on'   => new \Hazaar\Date(),
            'modified_on'  => null
        );

        if(($id = $this->db->file->insert($info, 'id')) > 0){

            $info['id'] = $id;

            if(! array_key_exists('items', $parent))
                $parent['items'] = array();

            $parent['items'][$info['filename']] = $info;

            return true;

        }

        return false;

    }

    public function unlink($path) {

        if($info = $this->info($path)) {

            $parent =& $this->info(dirname($path));

            if($info['kind'] != 'dir')
                $this->db->file_chunk->delete(array('file_id' => $info['id']));

            if($this->db->file->delete(array('id' => $info['id']))){

                unset($parent['items'][$info['filename']]);

                return true;

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

            if(in_array($parent['id'], $info['parent']))
                return false;

            $data = array(
                'modifiedDate' => new \Hazaar\Date(),
                'parent' => $parent['_id'])
            );

            $ret = $this->collection->update(array('_id' => $info['_id']), $data);

            if($ret['ok'] == 1) {

                if(! array_key_exists('items', $parent))
                    $parent['items'] = array();

                $parent['items'][$info['filename']] = $info;

                return true;

            }

        } else {

            $fileInfo = array(
                'kind'         => 'file',
                'parents'      => array($parent['_id']),
                'filename'     => basename($path),
                'mime_type'    => $content_type,
                'modifiedDate' => null,
                'md5'          => $md5
            );

            if($info = $this->info($path))
                $fileInfo['meta'] = ake($info, 'meta');

            if($id = $this->gridFS->storeBytes($bytes, $fileInfo)) {

                $fileInfo['_id'] = $id;

                $fileInfo['length'] = strlen($bytes);

                if(! array_key_exists('items', $parent))
                    $parent['items'] = array();

                $parent['items'][$fileInfo['filename']] = $fileInfo;

                return true;

            }

        }

        return false;

    }

    public function upload($path, $file, $overwrite = false) {

        $parent =& $this->info($path);

        if(! $parent)
            return false;

        $md5 = md5_file($file['tmp_name']);

        if($info = $this->db->file->findOne(array('md5' => $md5))) {

            if(in_array($parent['id'], $info['parent']))
                return false;

            $data = array(
                'modified_on' => new \Hazaar\Date(),
                'parent' => $parent['id']
            );

            $ret = $this->db->file->update(array('id' => $info['id']), $data);

            if($ret['ok'] == 1) {

                if(! array_key_exists('items', $parent))
                    $parent['items'] = array();

                $parent['items'][$info['filename']] = $info;

                return true;

            }

        } else {

            $fileInfo = array(
                'kind'         => 'file',
                'parent'       => $parent['id'],
                'filename'     => $file['name'],
                'created_on'   => new \Hazaar\Date(),
                'modified_on'  => null,
                'length'       => $file['size'],
                'mime_type'    => $file['type'],
                'md5'          => $md5
            );

            if($id = $this->db->file->insert($fileInfo, 'id')) {

                $stmt = $this->db->prepare('INSERT INTO file_chunk (file_id, n, data) VALUES (?, ?, ?);');

                $stmt->bindParam(1, $id);

                $n = 0;

                $stmt->bindParam(2, $n); //Support for multiple chunks will come later at some point

                $stmt->bindParam(3, file_get_contents($file['tmp_name']), \PDO::PARAM_LOB);

                //Insert the data chunk.  If this fails then remote the file.  We do this without a transaction due to the potential size.
                if(!$stmt->execute()){

                    $this->db->file->delete(array('id' => $id));

                    throw new \Exception($this->db->errorInfo()[2]);

                    return false;

                }

                $fileInfo['id'] = $id;

                $fileInfo['length'] = $file['size'];

                if(! array_key_exists('items', $parent))
                    $parent['items'] = array();

                $parent['items'][$fileInfo['filename']] = $fileInfo;

                return true;

            }

        }

        return false;

    }

    public function copy($src, $dst, $recursive = false) {

        die(__METHOD__);

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
            '$set' => array('modifiedDate' => new \MongoDate)
        );

        if(! in_array($dstParent['_id'], $source['parents']))
            $data['$push'] = array('parents' => $dstParent['_id']);

        $ret = $this->collection->update(array('_id' => $source['_id']), $data);

        if($ret['ok'] == 1) {

            if(! array_key_exists('items', $dstParent))
                $dstParent['items'] = array();

            $dstParent['items'][$source['filename']] = $source;

            return true;

        }

        return false;

    }

    public function link($src, $dst) {

        die(__METHOD__);

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
            '$set' => array('modifiedDate' => new \MongoDate)
        );

        if(! in_array($dstParent['_id'], $source['parents']))
            $data['$push'] = array('parents' => $dstParent['_id']);

        $ret = $this->collection->update(array('_id' => $source['_id']), $data);

        if($ret['ok'] == 1) {

            if(! array_key_exists('items', $dstParent))
                $dstParent['items'] = array();

            $dstParent['items'][$source['filename']] = $source;

            return true;

        }

        return false;

    }

    public function move($src, $dst) {

        die(__METHOD__);

        if(substr($dst, 0, strlen($src)) == $src)
            return false;

        if(! ($source = $this->info($src)))
            return false;

        $srcParent =& $this->info(dirname($src));

        $data = array(
            '$set' => array('modifiedDate' => new \MongoDate)
        );

        $dstParent =& $this->info($dst);

        if($dstParent) {

            //If the destination exists and is NOT a directory, return false so we don't overwrite an existing file.
            if($dstParent['kind'] !== 'dir')
                return false;

        } else {

            //We are renaming the file.

            if($source['filename'] != basename($dst))
                $dstParent['filename'] = $data['$set']['filename'] = basename($dst);

            $dstParent =& $this->info(dirname($dst));

            //Update the parents items array key with the new name.
            $basename = basename($src);

            $dstParent['items'][basename($dst)] = $dstParent['items'][$basename];

            unset($dstParent['items'][$basename]);

        }

        if(! in_array($dstParent['_id'], $source['parents']))
            $data['$push'] = array('parents' => $dstParent['_id']);

        $ret = $this->collection->update(array('_id' => $source['_id']), $data);

        if($ret['ok'] == 1) {

            if(! array_key_exists('items', $dstParent))
                $dstParent['items'] = array();

            $dstParent['items'][$source['filename']] = $source;

            if($srcParent['_id'] != $dstParent['_id']) {

                unset($srcParent['items'][$source['filename']]);

                $this->collection->update(array('_id' => $source['_id']), array('$pull' => array('parents' => $srcParent['_id'])));

            }

            return true;

        }

        return false;

    }

    public function chmod($path, $mode) {

        die(__METHOD__);

        if(! is_int($mode))
            return false;

        if($target =& $this->info($path)) {

            $target['mode'] = $mode;

            $ret = $this->collection->update(array('_id' => $target['_id']), array('$set' => array('mode' => $mode)));

            return ($ret['ok'] == 1);

        }

        return false;

    }

    public function chown($path, $user) {

        die(__METHOD__);

        if($target =& $this->info($path)) {

            $target['owner'] = $user;

            $ret = $this->collection->update(array('_id' => $target['_id']), array('$set' => array('owner' => $user)));

            return ($ret['ok'] == 1);

        }

        return false;

    }

    public function chgrp($path, $group) {

        die(__METHOD__);

        if($target =& $this->info($path)) {

            $target['group'] = $group;

            $ret = $this->collection->update(array('_id' => $target['_id']), array('$set' => array('group' => $group)));

            return ($ret['ok'] == 1);

        }

        return false;

    }

    public function set_meta($path, $values) {

        die(__METHOD__);

        if($target =& $this->info($path)) {

            $data = array();

            foreach($values as $key => $value)
                $data['meta.' . $key] = $value;

            $ret = $this->collection->update(array('_id' => $target['_id']), array('$set' => $data));

            return ($ret['ok'] == 1);

        }

        if($parent =& $this->info(dirname($path))) {

            $parent['items'][basename($path)]['meta'] = $values;

            return true;

        }

        return false;

    }

    public function get_meta($path, $key = null) {

        die(__METHOD__);

        if(! ($info = $this->info($path)))
            return false;

        if(array_key_exists('meta', $info)) {

            if($key)
                return ake($info['meta'], $key);

            return $info['meta'];

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
