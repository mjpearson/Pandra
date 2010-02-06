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
class PandraColumnFamily extends PandraColumnContainer {

    /* @var string keyspace (or database) for this column family */
    protected $keySpace = NULL;

    /* @var this column families name (table name) */
    protected $name = NULL;

    /* @var mixed keyID key for the working row */
    public $keyID = NULL;

    /* var bool object was loaded from Cassandra */
    protected $_loaded = FALSE;

    /**
     * Constructor, builds column structures
     */
    public function __construct($keyID = NULL) {
        parent::__construct();
        if ($keyID !== NULL) {
            $this->load($keyID);
        }
    }

    /**
     * marks the column family for this key, for deletion from the keyspace
     * operation cascades to columns
     * @return void
     */
    public function delete() {
        $this->setDelete(TRUE);
        foreach ($this->_columns as &$column) {
            $column->delete();
        }
    }

    /**
     * unmarks column family for deletion
     * cascades to columns, unsets modified flag
     */
    public function reset() {
        $this->setDelete(FALSE);
        foreach ($this->_columns as &$column) {
            $column->reset();
        }
        return (!$this->getDelete() && !$this->isModified());
    }

    /**
     * @return bool Column Family is marked for deletion
     */
    public function isDeleted() {
        return ($this->isModified() && $this->getDelete());
    }

    /**
     * Loads an entire columnfamily by keyid
     * @param string $keyID row key
     * @param bool $colAutoCreate create columns in the object instance which have not been defined
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID, $colAutoCreate = NULL, $consistencyLevel = NULL) {

        $this->_loaded = FALSE;

        $result = PandraCore::getCFSlice($keyID, $this->getKeySpace(), $this->getName(), NULL, PandraCore::getConsistency($consistencyLevel));

        if ($result !== NULL) {
            $this->init();
            $this->_loaded = $this->populate($result, $this->getAutoCreate($colAutoCreate));
            if ($this->_loaded) $this->setKeyID($keyID);
        } else {
            $this->registerError(PandraCore::$lastError);
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
        if ($this->getKeySpace() === NULL) throw new RuntimeException('NULL keySpace defined, cannot insert');

        // check a column family is defined
        if ($this->getName() === NULL) throw new RuntimeException('NULL name defined, cannot insert');
    }

    /**
     * Save this column family and any modified columns to Cassandra
     * @param cassandra_ColumnPath $columnPath
     * @param int $consistencyLevel Cassandra consistency level
     * @return bool save ok
     */
    public function save($consistencyLevel = NULL) {

        $this->checkCFState();

        $ok = FALSE;

        if ($this->getDelete()) {

            $columnPath = new cassandra_ColumnPath();
            $columnPath->column_family = $this->getName();

            $ok = PandraCore::deleteColumnPath($this->getKeySpace(), $this->keyID, $columnPath, time());
            if (!$ok) $this->registerError(PandraCore::$lastError);

        } else {

            foreach ($this->_columns as &$cObj) {
                if (!$cObj->isModified()) continue;
                if (!$cObj->save($this->keyID, $this->getKeySpace(), $this->getName(), PandraCore::getConsistency($consistencyLevel))) {
                    $this->registerError($cObj->getLastError());
                    return FALSE;
                }
            }
            $ok = TRUE;
        }

        if ($ok) $this->reset();

        return $ok;
    }

    /**
     * accessor method
     * @return bool this Column Family has been loaded from Cassandra
     */
    public function isLoaded() {
        return $this->_loaded;
    }

    public function setName($name) {
        $this->name = $name;
    }

    public function getName() {
        return $this->name;
    }

    public function setKeySpace($keySpace) {
        $this->keySpace = $keySpace;
    }

    public function getKeySpace() {
        return $this->keySpace;
    }

    public function setKeyID($keyID) {
        $this->keyID = $keyID;
    }

    public function getKeyID() {
        return $this->keyID;
    }
}
?>