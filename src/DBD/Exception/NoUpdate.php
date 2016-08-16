<?php

namespace Hazaar\DBI\DBD\Exception;

class NoUpdate extends \Hazaar\Exception {

    function __construct() {

        parent::__construct("No columns are being updated!");

    }

}

