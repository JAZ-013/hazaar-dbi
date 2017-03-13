<?php

namespace Hazaar\DBI;

class Console extends \Hazaar\Console\Module {

    public function init(){

        $this->addMenuGroup('dbi', 'Databases');

        $this->addMenuItem('dbi', 'Schema Migration', 'schema');

        $this->addMenuItem('dbi', 'Data Sync', 'sync');

        $this->view->requires('dbi.js');

    }

    public function schema(){

        $db = new \Hazaar\DBI\Adapter();

        $this->view('schema');

        $current = $db->getSchemaVersion();

        $versions = array('latest' => 'Latest Version') + $db->getSchemaVersions();

        $this->view->current_version = $current . ' - ' . ake($versions, $current, 'missing');

        $this->view->versions = $versions;

        $this->view->latest = $db->isSchemaLatest();

    }

    public function snapshot($request){

        if(!$request->isPOST())
            return false;

        $db = new \Hazaar\DBI\Adapter();

        $result = $db->snapshot($request->get('comment'), boolify($request->get('testmode', false)));

        return array('ok' => $result, 'log' => $db->getMigrationLog());

    }

    public function migrate($request){

        if(!$request->isPOST())
            return false;

        $version = $this->request->get('version', 'latest');

        if($version == 'latest')
            $version = null;

        $db = new \Hazaar\DBI\Adapter();

        $result = $db->migrate($version, boolify($request->get('testmode', false)));

        return array('ok' => $result, 'log' => $db->getMigrationLog());

    }

    public function sync($request){

        $this->view('sync');

    }

    public function syncdata($request){

        if(!$request->isPOST())
            return false;

        $db = new \Hazaar\DBI\Adapter();

        $result = $db->syncSchemaData();

        return array('ok' => $result, 'log' => $db->getMigrationLog());

    }

}