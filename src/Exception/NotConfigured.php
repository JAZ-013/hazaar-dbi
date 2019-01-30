<?php

namespace Hazaar\DBI\Exception;

class NotConfigured extends \Hazaar\Exception {

    function __construct() {

        parent::__construct('The DBI adapter has not been configured!');

    }

}
