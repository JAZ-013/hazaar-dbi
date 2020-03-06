<?php

namespace Hazaar\DBI\Exception;

class ConnectionFailed extends \Hazaar\Exception {

    function __construct($server) {

        parent::__construct("Database connection failed.  Error connecting to server: " . $server);

    }

}
