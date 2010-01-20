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
abstract class PandraColumnFamily extends PandraColumnContainer {

    /* @var string keyspace (or database) for this column family */
    public $keySpace = NULL;

    /* @var this column families name (table name) */
    public $name = NULL;

    /* @var mixed keyID key for the working row */
    public $keyID = NULL;

    /* @var int column family type (standard or super) */
    const TYPE = PANDRA_STANDARD;

    /* var bool columnfamily marked for deletion */
    private $_delete = FALSE;

    /* var bool object was loaded from Cassandra */
    private $_loaded = FALSE;

    /**
     * marks the column family for this key, for deletion from the keyspace
     * operation cascades to columns
     * @return void
     */
    public function delete() {
        $this->_delete = TRUE;
        foreach ($this->_columns as &$column) {
            $column->delete();
        }
    }

    /**
     * unmarks column family for deletion
     * cascades to columns, unsets modified flag
     */
    public function reset() {
        $this->_delete = FALSE;
        foreach ($this->_columns as &$column) {
            $column->reset();
        }
    }

    /**
     * @return bool Column Family is marked for deletion
     */
    public function isDeleted() {
        return $this->_delete;
    }

    /**
     * Gets complete slice of Thrift cassandra_Column objects for keyID
     *
     * @return array cassandra_Column objects
     */
    public function getRawSlice($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        $this->keyID = $keyID;

        $this->checkCFState();

        $client = Pandra::getClient();

        // build the column path
        $columnParent = new cassandra_ColumnParent();
        $columnParent->column_family = $this->name;
        //$columnParent->super_column = $this->superColumn;
        $columnParent->super_column = null;

        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = new cassandra_SliceRange();
        $predicate->slice_range->start = '';
        $predicate->slice_range->finish = '';

        return $client->get_slice($this->keySpace, $keyID, $columnParent, $predicate, $consistencyLevel);
    }

    /**
     * Loads a row by its keyID
     * @param string $keyID row key
     * @param bool $colAutoCreate create columns in the object instance which have not been defined
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID, $colAutoCreate = FALSE, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        $this->_loaded = FALSE;

        $result = $this->getRawSlice($keyID, $consistencyLevel);
        if (!empty($result)) {
            foreach ($result as $cObj) {
                // populate self, skip validators - self trusted
                if ($colAutoCreate && !array_key_exists($cObj->column->name, $this->_columns)) $this->addColumn($cObj->column->name);
                $this->_columns[$cObj->column->name]->value = $cObj->column->value;
            }
            $this->_loaded = TRUE;
        }
        return $this->_loaded;
    }

    /**
     * Check column family object state looks OK for writes
     * @todo: make more dev friendly
     */
    public function checkCFState() {
        // check a keyID is defined
        if ($this->keyID === NULL) throw new RuntimeException('NULL keyID defined, cannot insert');

        // check a Keyspace is defined
        if ($this->keySpace === NULL) throw new RuntimeException('NULL keySpace defined, cannot insert');

        // check a column family is defined
        if ($this->name === NULL) throw new RuntimeException('NULL name defined, cannot insert');
    }

    /**
     * Save this column family and any modified columns to Cassandra
     * @param cassandra_ColumnPath $columnPath
     * @param int $consistencyLevel Cassandra consistency level
     * @return bool save ok
     */
    public function save(cassandra_ColumnPath $columnPath = NULL, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        $this->checkCFState();

        if ($this->_delete === TRUE) {
            $client = Pandra::getClient(TRUE);

            if ($columnPath === NULL) {
                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->name;
            }
            
            // Delete this column family
            $client->remove($this->keySpace, $this->keyID, $columnPath, time(), $consistencyLevel);
        }

        foreach ($this->_columns as &$cObj) {
            if (!$cObj->isModified()) continue;
            if (!$cObj->save()) return FALSE;
        }

        $this->reset();
        return TRUE;
    }

    /**
     * accessor method
     * @return bool this Column Family has been loaded from Cassandra
     */
    public function isLoaded() {
       return $this->_loaded;
    }
}
?>