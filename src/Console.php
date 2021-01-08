<?php

namespace Hazaar\DBI;

class Console extends \Hazaar\Console\Module {

    private $db;

    public function load(){

        $this->db = \Hazaar\DBI\Adapter::getInstance();

        $group = $this->addMenuItem('Databases', 'database');

        if($this->db->ready()){

            $group->addMenuItem('Migration', 'migrate', 'random');

            $group->addMenuItem('Snapshot', 'snapshot', 'camera');

            $group->addMenuItem('Data Sync', 'sync', 'refresh');

            $group->addMenuItem('File System', 'fsck', 'folder');

        }

        return $group;
        
    }

    public function init(){

        $this->view->link('css/main.css');

        $this->view->requires('js/dbi.js');

        $this->notice('This module is currently under active development!', 'exclamation-triangle', 'warning');

        $manager = $this->db->getSchemaManager();

        $current = $manager->getVersion();

        $versions = array('latest' => 'Latest Version') + $manager->getVersions();

        $this->view->version_info = array(
            'current' => ($current ? $current . ' - ' . ake($versions, $current, 'missing') : 'Not Managed'),
            'managed' => ($current !== false),
            'latest' => $manager->isLatest(),
            'updates' => $manager->getMissingVersions()
        );

    }

    public function index(){

        $this->view('settings');

    }

    public function migrate(){

        if($this->request->isPOST()){

            $version = $this->request->get('version', 'latest');

            if($version == 'latest')
                $version = null;

            $result = false;

            try{

                $result = $this->db->getSchemaManager()->migrate(
                    $version,
                    boolify($this->request->get('sync')),
                    boolify($this->request->get('testmode', false)),
                    boolify($this->request->get('keeptables', false))
                );

                $log = $this->db->getSchemaManager()->getMigrationLog();

            }catch(\Throwable $e){

                $log = $this->db->getSchemaManager()->getMigrationLog();

                $log[] = array('time' => time(), 'msg' => $e->getMessage() . ' in file ' . $e->getFile() . ' on line #' . $e->getLine() . '.');

            }

            return array('ok' => $result, 'log' => $log);

        }

        $this->view('migrate');

        $versions = $this->db->getSchemaManager()->getVersions();

        foreach($versions as $ver => &$name)
            $name = $ver . ' - ' . $name;

        krsort($versions);

        $this->view->versions = array('latest' => 'Latest Version') + $versions;

    }

    public function snapshot(){

        if($this->request->isPOST()){

            $result = $this->db->getSchemaManager()->snapshot($this->request->get('comment'), boolify($this->request->get('testmode', false)));

            return array('ok' => $result, 'log' => $this->db->getSchemaManager()->getMigrationLog());

        }

        $this->view('snapshot');

    }

    public function sync(){

        if($this->request->isPOST()){

            $result = $this->db->getSchemaManager()->syncData();

            return array('ok' => $result, 'log' => $this->db->getSchemaManager()->getMigrationLog());

        }

        $this->view('sync');

    }

    public function fsck(){

        if($this->request->isPOST()){

            \set_time_limit(300);

            $result = false;

            if($m = \Hazaar\File\Manager::select($this->request->fs))
                $result = $m->fsck();

            return array('ok' => $result);

        }

        $this->view('files');

        $filesystems = array();

        $config = new \Hazaar\Application\Config('media');

        foreach($config as $name => $info){

            if($info['type'] === 'DBI')
                $filesystems[$name] = $info->toArray();

        }

        $this->view->filesystems = $filesystems;

    }

}