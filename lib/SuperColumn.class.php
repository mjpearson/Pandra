<?php
/**
 * SuperColumn container.
 *
 * @package pandra
 */
class PandraSuperColumn extends PandraColumnFamily {

    /* @var array container for child column family objects, indexed to cf name */
    protected $columnFamilies = array();

    /* @var PandraColumnFamily column family parent reference */
    private $_parentCF = NULL;

    public function __construct($superName, $parentCF) {
        $this->superColumn = $superName;
        $this->_parentCF = $parentCF;
    }

    public function constructColumns() { }

    public function addColumnFamily($cfName, PandraColumnFamily $cfObj = NULL) {
                
        if ($this->_parentCF instanceof PandraColumnFamily) {
            // Will get here if dimensions is key > SuperColumn > ColumnFamily > SuperColumn
            throw new RuntimeException('Cannot add ColumnFamily: Dimensionality Exceeded');
        }

        if ($cfObj === NULL) {
            $cfObj = new PandraColumnFamily($this->keyID);
        }

        $this->columnFamilies[$cfName] = $cfObj;

        return $this->getColumnFamily($cfName);
    }

    public function removeColumnFamily() {     
    }

    public function getColumnFamily($cfName) {
        if (array_key_exists($cfName, $this->columnFamilies)) return $this->columnFamilies[$cfName];
        return NULL;
    }

    public function addSuper($superName) {
        throw new RuntimeException("SuperColumns cannot be containers for SuperColumns");
    }

    public function getSuper($superName) {
        return NULL;
    }
 
}