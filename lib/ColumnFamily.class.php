<?php
/**
 * ColumnFamily container class
 * @package Pandra
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
    private $_type = PANDRA_CF_STANDARD;

      /* var bool columnfamily marked for deletion */
    private $_delete = FALSE;

    private $_loaded = FALSE;

    /**
     * Constructor, builds column structures
     */
    public function ___construct($keyID = NULL) {
        $this->constructColumns();
        if ($keyID !== NULL) $this->load($keyID);
    }

    /**
     * deletes the loaded object from the keyspace, or optionally the supplied columns for the key
     */
    public function markDelete() {
        $this->_delete = TRUE;
        foreach ($this->columns as &$column) {
            $column->delete();
        }
    }

    public function delete() {
        $this->markDelete();
    }

    public function reset() {
        $this->_delete = FALSE;
        foreach ($this->columns as &$column) {
            $column->reset();
        }
    }

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
     * Loads a row by it's keyID (all supercolumns and columns)
     * @todo super columns and slice loads
     * @param mixed $value value of this rows primary key to load from
     * @return bool this object has loaded its fields
     */
    public function load($keyID, $colAutoCreate = FALSE, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        $this->_loaded = FALSE;

        $result = $this->getRawSlice($keyID, $consistencyLevel);
        if (!empty($result)) {
            foreach ($result as $cObj) {
                // populate self, skip validators - self trusted
                if ($colAutoCreate && !array_key_exists($cObj->column->name, $this->columns)) $this->addColumn($cObj->column->name);
                $this->columns[$cObj->column->name]->value = $cObj->column->value;
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
     * Save all columns in this loaded columnfamily
     * @return void
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

        foreach ($this->columns as &$cObj) {
            if (!$cObj->save()) return FALSE;
        }
        return TRUE;
    }

    public function getType() {
        return $this->_type;
    }

    public function isLoaded() {
       return $this->_loaded;
    }
}
?>