<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @link http://www.phpgrease.net/projects/pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */

abstract class PandraClause {

    const TYPE_LITERAL = 0;

    const TYPE_RANGE = 1;

    protected $_type = NULL;

    public function match($value) {}

    public function getType() {
        return $this->_type;
    }
}
?>