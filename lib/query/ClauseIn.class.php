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

class PandraClauseIn extends PandraClause {

    protected $_type = self::TYPE_LITERAL;

    private $_valueIn = array();

    public function __construct() {
        $args = func_get_arg(0);

        if (!is_array($args)) $args = array($args);
        $this->_valueIn = $args;
    }

    public function match($value) {
        return (in_array($value, $this->_valueIn));
    }
}
?>