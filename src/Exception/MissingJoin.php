<?php

namespace Hazaar\DBI\Exception;

class MissingJoin extends \Hazaar\Exception {

    function __construct($ref) {

        parent::__construct("Missing join while referencing '$ref'.");

    }

}
