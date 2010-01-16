<?php

abstract class PandraColumnContainer {

    /* @var this column families name (table name) */
    public $name = NULL;

    /* @var mixed keyID key for the working row */
    public $keyID = NULL;

    /* @var string magic set/get prefixes for Columns */
    const _columnNamePrefix = 'column_';	// magic __get/__set column prefix in column famliy

    /* @var string magic get/set prefix for Super Columns */
    const _superNamePrefix = 'super_';

    /* @var array container for column objects, indexed to field name */
    protected $columns = array();

    /* @var array complete list of errors for this object instance */
    public $errors = array();

    /* var bool columnfamily marked for deletion */
    private $_delete = FALSE;

    /**
     * Constructor, builds column structures
     */
    public function __construct($keyID = NULL) {
        $this->constructColumns();
        if ($keyID !== NULL) $this->load($keyID);
    }

    /**
     * constructFields builds column objects via addColumn/addSuper methods for defined schemas
     */
    public function constructColumns() {}

    public function lastError() {
        if (!empty($this->errors)) {
            return $this->errors[0];
        }
        return NULL;
    }

    public function addColumn($colName, $typeDef = array(), $callbackOnSave = NULL) {
        if (!array_key_exists($colName, $this->columns)) {
            $this->columns[$colName] = new PandraColumn($colName, $this, $typeDef);
        }

        // pre-save callback
        if (!empty($callbackOnSave)) $this->columns[$colName]->callback = $callbackOnSave;

        return $this->getColumn($colName);
    }

    public function getColumn($colName) {
        if (array_key_exists($colName, $this->columns)) {
            return $this->columns[$colName];
        }
        return NULL;
    }

    /**
     * Sets a columns value for this slice
     * @param string $colName Column name to set
     * @param string $value New value for column
     * @param bool $validate opt validate from typeDef (default TRUE)
     * @return bool column set ok
     */
    public function setColumn($colName, $value, $validate = TRUE) {
        return (array_key_exists($colName, $this->columns) && $this->columns[$colName]->setValue($value, $validate));
    }

    public function listColumns() {
        return array_keys($this->columns);
    }

   /**
     * Gets complete slice of Thrift cassandra_Column objects for keyID
     *
     * @return array cassandra_Column objects
     */
    public function getRawSlice($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        $this->keyID = $keyID;

        $this->_parentCF->checkCFState();

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
     * Resets deletion states for the column family
     */
    public function reset() {
        // undo any deletion marks
        $this->_delete = FALSE;
        foreach  ($this->columns as &$cObj) {
            $cObj->reset();
        }
    }

    /*
     * Populates object from $data array.  Bool false on validation error error, check $this->errors for messages
     * @param array key/value pair of column => value
     * @return bool populated without validation errors
    */
    public function populate($data) {
        if (is_string($data)) {
            $data = json_decode($data, TRUE);
        }

        if (is_array($data)) {
            foreach ($data as $key => $value) {
                if (array_key_exists($key, $this->columns)) {                    
                    $this->columns[$key]->setValue($value);
                }
            }
        } else {
            return FALSE;
        }
        
        return empty($this->errors);
    }

    /**
     * determine if get/set field exists/is mutable, strips field prefix from magic get/setters
     * @param string $colName field name to check
     * @return bool field exists
     */
    private function gsMutable(&$colName) {
        $colName = preg_replace("/^".self::_columnNamePrefix."/", "", strtolower($colName));
        return array_key_exists($colName, $this->columns);
    }

    /**
     * Magic getter
     * @param string $colName field name to get
     * @return string value
     */
    protected function __get($colName) {
        if ($this->gsMutable($colName)) {
            return $this->columns[$colName]->value;
        } else {
            return NULL;
        }
    }

    /**
     * Magic setter
     * @todo propogate an exception for setcolumn if it returns false.  magic __set's are void return type
     * @param string $colName field name to set
     * @param string $value  value to set for field
     * @return bool field set ok
     */
    protected function __set($colName, $value) {
        if ($this->gsMutable($colName)) {
            $this->setColumn($colName, $value);
        }
    }    
}