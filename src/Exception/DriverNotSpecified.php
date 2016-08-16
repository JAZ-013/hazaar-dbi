<?php

namespace Hazaar\DBI\Exception;

class DriverNotSpecified extends \Hazaar\Exception {

    function __construct() {

        parent::__construct('No database driver specified!');

    }

}
