<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */
class PandraColumn extends cassandra_Column {
    
    /* @var string column name */
    public $name = NULL;

    /* @var string column value */
    //private $_value = NULL;
    public $value = NULL;

    /* @var int last changed timestamp */
    public $timestamp = NULL;

    /* @var array validator type definitions for this colun */
    public $typeDef = array();

    /* @var string last processing error */
    public $lastError = array();

    /* @var string callback function for this column pre-save */
    public $callback = NULL;

    /* @var bool column value has been modified since load() or init */
    private $_modified = FALSE;

    /* @var $delete column is marked for deletion */
    private $_delete = FALSE;

    /* @var PandraColumnFamily column family parent reference */
    private $_parentCF = NULL;

    /**
     * Column constructor (extends cassandra_Column)
     * @param string $name Column name
     * @param PandraColumnContainer $parentCF parent column family (standard or super), or supercolumn
     * @param array $typeDef validator type definitions
     */
    public function __construct($name, PandraColumnContainer $parentCF, $typeDef = array()) {
        parent::__construct(array('name' => $name));
        $this->setParentCF($parentCF);
        $this->typeDef = $typeDef;
    }

    /**
     * Sets parent ColumnFamily or
     * @param PandraColumnContainer $parentCF
     */
    public function setParentCF(PandraColumnContainer $parentCF) {
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
     * Binds a timestamp to the column, defaults to current time if no override defined
     * @param int $timeOverride new stamp
     * @return int new timestamp
     */
    public function bindTime($timeOverride = NULL) {
        $this->timestamp = ($timeOverride === NULL) ? time() : $timeOverride;
        $this->_modified = TRUE;
        return $this->timestamp;
    }

    /**
     * Sets the value of the column
     * @param mixed $value new value
     * @param bool $validate validate the value, if typeDef is set
     * @return bool column set ok (check lastError for details)
     */
    public function setValue($value, $validate = TRUE) {
        if ($validate && !empty($this->typeDef)) {
            if (!PandraValidator::check($value, $this->name, $this->typeDef, $this->lastError)) {
                $this->_parentCF->errors[] = $this->lastError[0];
                return FALSE;
            }
        }

        $this->value = $value;
        $this->_modified = TRUE;
        return TRUE;
    }

    public function callbackvalue() {
        $value = '';
        eval('$value = '.$this->callback.'("'.$this->value.'");');
        return $value;
    }

    public function cast($object, $parentCF = NULL) {
        if (get_class($object) == 'cassandra_Column') {
            $newObj = new PandraColumn($object->name, ($parentCF === NULL) ? $this->_parentCF : $parentCF);
            $newObj->value = $object->value;
            $newObj->timestamp = $object->timestamp;
            return $newObj;
        }
        return NULL;
    }

    public function save(cassandra_ColumnPath $columnPath = NULL, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        if (!$this->isModified()) return TRUE;

        //if (!$this->_modified) return FALSE;
        $this->_parentCF->checkCFState();

        $client = Pandra::getClient(TRUE);

        // build the column path
        if ($columnPath == NULL) {
            $columnPath = new cassandra_ColumnPath();
        }
        $columnPath->column_family = $this->_parentCF->name;
        $columnPath->column = $this->name;

        try {
            if ($this->_delete) {
                $client->remove($this->_parentCF->keySpace, $this->_parentCF->keyID, $columnPath, $this->bindTime(), $consistencyLevel);
            } else {
                $client->insert($this->_parentCF->keySpace, $this->_parentCF->keyID, $columnPath, ($this->callback === NULL) ? $this->value : $this->callbackvalue(), $this->bindTime(), $consistencyLevel);
            }
        } catch (TException $te) {
            array_push($this->lastError, $te->getMessage());
            $this->_parentCF->errors[] = $this->lastError[0];
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Removes any modified or delete flags, (does not revert values)
     */
    public function reset() {
        $this->_modified = FALSE;
        $this->_delete = FALSE;
    }

    /**
     * Marks this column for deletion
     */
    public function delete() {
        $this->_delete = TRUE;
        $this->_modified = TRUE;        
    }

    /**
     * @return bool Column is marked for deletion
     */
    public function isDeleted() {
        return $this->_delete;
    }

    /**
     * @return bool column has been modified since instance construct/load
     */
    public function isModified() {
        return $this->_modified;
    }
}