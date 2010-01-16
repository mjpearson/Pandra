<?php
/**
 * SuperColumn container.
 *
 * @package pandra
 */
class PandraSuperColumn extends PandraColumnContainer {

    /* @var PandraColumnFamily column family parent reference */
    private $_parentCF = NULL;

    public function __construct($superName, PandraColumnFamilySuper $parentCF = NULL) {
        // SuperColumn name
        $this->name = $superName;

        // Reference parent ColumnFamilySuper
        if ($parentCF !== NULL) {
            $this->_parentCF = $parentCF;
        }
        parent::__construct();
    }

    public function setParentCF($parentCF) {
        $this->_parentCF = $parentCF;
    }

     /**
     * Save all columns in this loaded columnfamily
     * @return void
     */
    public function save() {

        $this->checkCFState();

        if ($this->_delete === TRUE) {
            $client = Pandra::getClient(TRUE);

            if ($columnPath === NULL) {
                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->name;
            }

            //$client->remove($this->keySpace, $this->keyID, $columnPath, time(), $consistencyLevel);
            echo 'REMOVING WHOLE COLUMNFAMILY FOR '.$this->keyID;
        }

        foreach ($this->columns as &$cObj) {
            echo 'SC SAVING '. $cObj->name;
            //var_dump($cObj);
            //$cObj->save();
        }
    }

}