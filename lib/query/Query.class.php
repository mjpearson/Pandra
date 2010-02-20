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

class PandraQuery {

    public $columns = array();

    // @todo move to callStatic with PHP5.3 upgrade
    public function __call($class, $args) {
        $class = 'PandraClause'.$class;
        if (class_exists($class)) {
            return new $class(array_pop($args));
        }
    }

    /**
     * Query stub (predicate search/slice range etc)
     */
    public function get() {
        return $this->columns;
    }

}
?>