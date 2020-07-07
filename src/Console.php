<?php

namespace Hazaar\DBI;

class Console extends \Hazaar\Console\Module {

    private $db;

    public function load(){

        $this->addMenuGroup('Databases', 'database');

        $this->addMenuItem('Migration', 'migrate', 'random');

        $this->addMenuItem('Snapshot', 'snapshot', 'camera');

        $this->addMenuItem('Data Sync', 'sync', 'refresh');

        $this->addMenuItem('File System', 'fsck', 'folder');

        $this->view->requires('js/dbi.js');

        $this->notice('This module is currently under active development!', 'exclamation-triangle', 'warning');

    }

    public function prepare(){

        $this->view->link('css/main.css');

        $this->db = new \Hazaar\DBI\Adapter();

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