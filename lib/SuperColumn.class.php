<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
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
     * @todo DELETES
     * @return void
     */
    public function save($consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        if (!$this->isModified()) return FALSE;

        $this->_parentCF->checkCFState();

        $ok = FALSE;

        if ($this->getDelete()) {

            if ($columnPath === NULL) {
                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->_parentCF->getName();
                $columnPath->super_column = $this->getName();

                $ok = Pandra::delete($this->keySpace, $this->keyID, $columnPath, time(), $consistencyLevel);
                if (!$ok) $this->registerError(Pandra::$lastError);

                return $ok;
            }

        } else {

            $this->bindTimeModifiedColumns();

            try {
                $mutation = array();

                $client = Pandra::getClient();

                // Cast this supercolumn into a thrift cassandra_SuperColumn()  (YICK!)
                $thisSuper = new cassandra_SuperColumn();
                $thisSuper->name = $this->getName();
                $thisSuper->columns = $this->getModifiedColumns();

                $scContainer = new cassandra_ColumnOrSuperColumn();
                $scContainer->super_column = $thisSuper;

                // @todo - move this to the columnfamilysuper class? Looks like it can handle multiple mutations
                $mutations[$this->_parentCF->getName()] = array($scContainer);
                $client->batch_insert($this->_parentCF->getKeySpace(), $this->_parentCF->keyID, $mutations, $consistencyLevel);

                $this->reset();

                return TRUE;

            } catch (TException $te) {
                $this->registerError($te->getMessage());
            }
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
    public function load($keyID, $colAutoCreate = PANDRA_DEFAULT_CREATE_MODE, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        if ($this->_parentCF == NULL || !($this->_parentCF instanceof PandraColumnFamilySuper)) throw new RuntimeException('SuperColumn Requires a ColumnFamilySuper parent');

        $this->_parentCF->checkCFState();

        $this->_loaded = FALSE;

        $result = Pandra::getCFSlice($keyID, $this->_parentCF->getKeySpace(), $this->_parentCF->getName(), $this->getName(), $consistencyLevel);

        if ($result !== NULL) {
            $this->init();
            $this->_loaded = $this->populate($result, $colAutoCreate);
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

    public function registerError($errorStr) {
        if (empty($errorStr)) return;
        array_push($this->errors, $errorStr);
        if ($this->_parentCF !== NULL) $this->_parentCF->registerError($errorStr);
    }
}
?>