<?php

namespace Hazaar\DBI\Exception;

class DriverNotFound extends \Hazaar\Exception {

    function __construct($driver) {

        parent::__construct("Database driver '$driver' not found.");

    }

}
