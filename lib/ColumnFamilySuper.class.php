<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */

/**
 * @abstract
 */
abstract class PandraColumnFamilySuper extends PandraColumnFamily {

    /* @var int column family type (standard or super) */
    const TYPE = PANDRA_SUPER;

    /* @var string magic get/set prefix for Super Columns */
    const _superNamePrefix = 'super_';

    /**
     * determine if get/set field exists/is mutable, strips field prefix from magic get/setters
     * @param string $colName field name to check
     * @return bool field exists
     */
    protected function _gsMutable(&$colName) {
        $colName = preg_replace("/^".self::_superNamePrefix."/", "", strtolower($colName));
        return array_key_exists($colName, $this->_columns);
    }

    /**
     * Helper function to add a Super Column instance to this Super Column Family
     * @param PandraSuperColumn $scObj
     * @return PandraSuperColumn
     */
    public function addSuper(PandraSuperColumn $scObj) {
        $superName = $scObj->name;

        $scObj->setParentCF($this);
        $this->_columns[$superName] = $scObj;

        return $this->getColumn($superName);
    }
}
?>