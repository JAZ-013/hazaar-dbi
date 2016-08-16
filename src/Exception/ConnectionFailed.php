<?php

namespace Hazaar\DBI\Exception;

class ConnectionFailed extends \Hazaar\Exception {

    function __construct() {

        parent::__construct("Failed to connect to database.");

    }

}
