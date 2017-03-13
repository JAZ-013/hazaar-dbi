<?php

namespace Hazaar\DBI;

class Console extends \Hazaar\Console\Module {

    public function init(){

        $this->addMenuGroup('dbi', 'Databases');

        $this->addMenuItem('dbi', 'Schema Migration', 'schema');

        $this->addMenuItem('dbi', 'Data Sync', 'sync');

    }

    public function schema(){

    }

    public function sync(){

    }

}