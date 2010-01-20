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
        $this->name = $superName;

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
    public function save($consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        $this->_parentCF->checkCFState();

        $client = Pandra::getClient(TRUE);

        if ($this->_delete === TRUE) {
            if ($columnPath === NULL) {
                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->name;
            }

            //$client->remove($this->keySpace, $this->keyID, $columnPath, time(), $consistencyLevel);
            echo 'REMOVING WHOLE SUPERCOLUMN '.$this->name.' FOR '.$this->keyID;
        }

        // Build batch insert/remove clauses
        $insertColumns = array();
        $removeColumns = array();

        $scModified = FALSE;
        foreach ($this->_columns as &$cObj) {
            if ($cObj->isModified()) {
                $scModified = TRUE;
                $cObj->bindTime();

                // Flag Deleting or inserting columns
                if ($cObj->isDeleted()) {
                    $removeColumns[] = $cObj;
                } else {
                    $insertColumns[] = $cObj;
                }
            }
        }
        
        if ($scModified && !empty($insertColumns)) {
            try {
                $mutation = array();

                // Cast this supercolumn into a thrift cassandra_SuperColumn()  (YICK!)
                $thisSuper = new cassandra_SuperColumn();
                $thisSuper->name = $this->name;
                $thisSuper->columns = $insertColumns;

                $scContainer = new cassandra_ColumnOrSuperColumn();
                $scContainer->super_column = $thisSuper;

                // @todo - move this to the columnfamilysuper class? Looks like it can handle multiple mutations
                $mutations[$this->_parentCF->name] = array($scContainer);
                $client->batch_insert($this->_parentCF->keySpace, $this->_parentCF->keyID, $mutations, $consistencyLevel);
                
            } catch (TException $te) {
                array_push($this->errors, $te->getMessage());
                $this->_parentCF->errors[] = $this->errors[0];
                return FALSE;
            }
        }
        return $scModified;
    }

    /**
     * Sets parent ColumnFamily or
     * @param PandraColumnContainer $parentCF
     */
    public function setParentCF(PandraColumnContainer $parentCF) {
        if (!($parentCF instanceof PandraColumnFamily) && !($parentCf instanceof PandraColumnFamilySuper)) {
            throw new RuntimeException('Parent must be a Column Family');
        }
        $this->_parentCF = $parentCF;
    }

    /**
     * Gets the current working parent column family
     * @return <type>
     */
    public function getParentCF() {
        return $this->_parentCF;
    }
}