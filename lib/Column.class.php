<?php
/**
 *
 * @package Pandra
 */
class PandraColumn {
    /* @var string column name */
    public $name = NULL;

    /* @var string column value */
    private $_value = NULL;

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

    public function __construct($name, $parentCF, $typeDef = array()) {
        //parent::__construct(array('name' => $name));
        $this->_parentCF = $parentCF;
        $this->typeDef = $typeDef;
    }

    public function bindTime($timeOverride = NULL) {
        $this->timestamp = ($timeOverride === NULL) ? time() : $timeOverride;
        return $this->timestamp;
    }

    public function setvalue($value, $validate = TRUE) {
        if ($validate && !empty($this->typeDef)) {
            if (!PandraValidator::check($value, $this->name, $this->typeDef, $this->lastError)) {
                $this->_parentCF->errors[] = $this->lastError[0];
                return FALSE;
            }
        }

        $this->_value = $value;
        $this->_modified = TRUE;
        return TRUE;
    }

    /**
     * Magic getter
     * @param string $colName field name to get
     * @return string value
     */
    public function __get($colName) {
        if ($colName == 'value') {
            return $this->_value;
        }
        return NULL;
    }

    /**
     * Magic setter
     * @todo propogate an exception for setcolumn if it returns false.  magic __set's are void return type
     * @param string $colName field name to set
     * @param string $value  value to set for field
     * @return bool field set ok
     */
    public function __set($colName, $value) {
        if ($colName == 'value') {
            return $this->setvalue($value, $this->_validator);
        }
        return NULL;
    }

    public function reset() {
        $this->_modified = FALSE;
        $this->_delete = FALSE;
    }

    public function markDelete() {
        $this->_delete = TRUE;
        $this->_modified = TRUE;
    }

    public function callbackvalue() {
        $value = '';
        eval('$value = '.$this->callback.'("'.$this->_value.'");');
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

        //if (!$this->_modified) return FALSE;
        $this->_parentCF->checkCFState();

        $client = Pandra::getClient(TRUE);

        // build the column path
        if ($columnPath == NULL) {
            $columnPath = new cassandra_ColumnPath();
        }
        $columnPath->column_family = $this->_parentCF->name;
        $columnPath->column = $this->name;

        // @todo super writes
        /*
		if ($this->_parentCF !== NULL && $this->_parentCF instanceof PandraSuperColumn) {
			$columnPath->super_column = $this->_parentCF->superColumn;
		}
        */
        try {
            if ($this->_delete) {
                $client->remove($this->_parentCF->keySpace, $this->_parentCF->keyID, $columnPath, $this->bindTime(), $consistencyLevel);
            } else {
                $client->insert($this->_parentCF->keySpace, $this->_parentCF->keyID, $columnPath, $this->_value, $this->bindTime(), $consistencyLevel);
            }
        } catch (TException $te) {
            array_push($this->lastError, $te->getMessage());
            $this->_parentCF->errors[] = $this->lastError[0];
            return FALSE;
        }
        return TRUE;
    }

    public function delete() {
        $this->markDelete();
        //$this->save();
    }

    public function isDeleted() {
        return $this->_delete;
    }

    public function isModified() {
        return $this->_modified;
    }
}