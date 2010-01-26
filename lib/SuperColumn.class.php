<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @author Michael Pearson <Pandra-support@phpgrease.net>
 */
class PandraSuperColumn extends PandraColumnContainer {

    /* @var PandraColumnFamily column family parent reference */
    private $_parentCF = NULL;

    /* @var int column family type (standard or super) */
    const TYPE = PANDRA_SUPER;

    /**
     * Supercolumn constructor
     * @param string $superName Super Column name
     * @param PandraColumnFamilySuper $parentCF
     */
    public function __construct($superName, PandraColumnFamilySuper $parentCF = NULL) {
        // SuperColumn name
        $this->setName($superName);

        // Reference parent ColumnFamilySuper
        if ($parentCF !== NULL) {
            $this->_parentCF = $parentCF;
        }
        parent::__construct();
    }

    /**
     * Save all columns in this loaded columnfamily
     * @return void
     */
    public function save($consistencyLevel = NULL) {

        if (!$this->isModified()) return FALSE;

        $this->_parentCF->checkCFState();

        $ok = FALSE;

        if ($this->getDelete()) {

            if ($columnPath === NULL) {
                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->_parentCF->getName();
                $columnPath->super_column = $this->getName();

                $ok = Pandra::delete($this->keySpace, $this->keyID, $columnPath, time(), Pandra::getConsistency($consistencyLevel));
                if (!$ok) $this->registerError(Pandra::$lastError);

                return $ok;
            }

        } else {

            $this->bindTimeModifiedColumns();
            $ok = Pandra::saveSuperColumn(  $this->_parentCF->getKeySpace(),
                                            $this->_parentCF->keyID,
                                            $this->_parentCF->getName(),
                                            $this->getName(),
                                            $this->getModifiedColumns(),
                                            Pandra::getConsistency($consistencyLevel));

            if (!$ok) $this->registerError(Pandra::$lastError);

            if ($ok) $this->reset();
            return $ok;
        }
        return FALSE;
    }

    /**
     * Loads a SuperColumn for key
     *
     * Load will generate RuntimeException if parent column family has not been set (
     *
     * @param string $keyID row key
     * @param bool $colAutoCreate create columns in the object instance which have not been defined
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID, $colAutoCreate = NULL, $consistencyLevel = NULL) {

        if ($this->_parentCF == NULL || !($this->_parentCF instanceof PandraColumnFamilySuper)) throw new RuntimeException('SuperColumn Requires a ColumnFamilySuper parent');

        $this->_parentCF->checkCFState();

        $this->_loaded = FALSE;

        $result = Pandra::getCFSlice($keyID, $this->_parentCF->getKeySpace(), $this->_parentCF->getName(), $this->getName(), Pandra::getConsistency($consistencyLevel));

        if ($result !== NULL) {
            $this->init();
            $this->_loaded = $this->populate($result, $this->getAutoCreate($colAutoCreate));
            if ($this->_loaded) $this->keyID = $keyID;
        } else {
            $this->registerError(Pandra::$lastError);
        }

        return $this->_loaded;
    }

    /**
     * Sets parent ColumnFamily or
     * @param PandraColumnContainer $parentCF
     */
    public function setParentCF(PandraColumnFamilySuper $parentCF) {
        $this->_parentCF = $parentCF;
    }

    /**
     * Gets the current working parent column family
     * @return <type>
     */
    public function getParentCF() {
        return $this->_parentCF;
    }

    /**
     * Creates an error entry in this column and propogate to parent
     * @param string $errorStr error string
     */
    public function registerError($errorStr) {
        if (!empty($errorStr)) {
            array_push($this->errors, $errorStr);
            if ($this->_parentCF !== NULL) $this->_parentCF->registerError($errorStr);
        }
    }
}
?>