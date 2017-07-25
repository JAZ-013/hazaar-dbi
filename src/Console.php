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

        $current = $this->db->getSchemaVersion();

        $versions = array('latest' => 'Latest Version') + $this->db->getSchemaVersions();

        $this->view->version_info = array(
            'current' => ($current ? $current . ' - ' . ake($versions, $current, 'missing') : null),
            'latest' => $this->db->isSchemaLatest()
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

            $result = $this->db->migrate($version, boolify($request->get('sync')), boolify($request->get('testmode', false)));

            return array('ok' => $result, 'log' => $this->db->getMigrationLog());

        }

        $this->view('migrate');

        $versions = array('latest' => 'Latest Version') + $this->db->getSchemaVersions();

        $this->view->versions = $versions;

    }

    public function snapshot($request){

        if($request->isPOST()){

            $result = $this->db->snapshot($request->get('comment'), boolify($request->get('testmode', false)));

            return array('ok' => $result, 'log' => $this->db->getMigrationLog());

        }

        $this->view('snapshot');

    }

    public function sync($request){

        if($request->isPOST()){

            $result = $this->db->syncSchemaData();

            return array('ok' => $result, 'log' => $this->db->getMigrationLog());

        }

        $this->view('sync');

    }

}