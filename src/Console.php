<?php

namespace Hazaar\DBI;

class Console extends \Hazaar\Console\Module {

    private $db;

    public function init(){

        $this->addMenuGroup('Databases', 'database');

        $this->addMenuItem('Migration', 'migrate', 'random');

        $this->addMenuItem('Snapshot', 'snapshot', 'camera');

        $this->addMenuItem('Data Sync', 'sync', 'refresh');

        $this->view->requires('js/dbi.js');

        $this->notice('This module is currently under active development!', 'exclamation-triangle', 'warning');

    }

    public function prepare(){

        $this->view->link('css/main.css');

        $this->db = new \Hazaar\DBI\Adapter();

        $current = $this->db->getSchemaManager()->getVersion();

        $versions = array('latest' => 'Latest Version') + $this->db->getSchemaManager()->getVersions();

        $this->view->version_info = array(
            'current' => ($current ? $current . ' - ' . ake($versions, $current, 'missing') : null),
            'latest' => $this->db->getSchemaManager()->isLatest()
        );

    }

    public function index(){

        $this->view('settings');

    }

    public function migrate($request){

        if($request->isPOST()){

            $version = $this->request->get('version', 'latest');

            if($version == 'latest')
                $version = null;

            $result = false;

            try{

                $result = $this->db->getSchemaManager()->migrate(
                    $version,
                    boolify($request->get('sync')),
                    boolify($request->get('testmode', false)),
                    boolify($request->get('keeptables', false))
                );

                $log = $this->db->getSchemaManager()->getMigrationLog();

            }catch(\Throwable $e){

                $log = $this->db->getSchemaManager()->getMigrationLog();

                $log[] = array('time' => time(), 'msg' => 'ERROR: ' . $e->getMessage() . ' in file ' . $e->getFile() . ' on line #' . $e->getLine() . '.');

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

    public function snapshot($request){

        if($request->isPOST()){

            $result = $this->db->getSchemaManager()->snapshot($request->get('comment'), boolify($request->get('testmode', false)));

            return array('ok' => $result, 'log' => $this->db->getSchemaManager()->getMigrationLog());

        }

        $this->view('snapshot');

    }

    public function sync($request){

        if($request->isPOST()){

            $result = $this->db->getSchemaManager()->syncData();

            return array('ok' => $result, 'log' => $this->db->getSchemaManager()->getMigrationLog());

        }

        $this->view('sync');

    }

}