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
abstract class PandraColumnContainer implements ArrayAccess {

    /* @var this column families name (table name) */
    public $name = NULL;

    /* @var mixed keyID key for the working row */
    public $keyID = NULL;

    /* @var string magic set/get prefixes for Columns */
    const _columnNamePrefix = 'column_';	// magic __get/__set column prefix in column famliy

    /* @var array container for column objects, indexed to field name */
    protected $_columns = array();

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
     * constructFields builds column objects via addColumn/addSuper methods
     * when defining hard schemas.  It is called automatically by the constructor.
     * @return void
     */
    public function constructColumns() {
    }

    /**
     * Sets value with a validator.  To skip validation, use explicit
     * PandraColumn->setValue($value, FALSE); or do not provide a typeDef
     * @param string $offset column name
     * @param mixed $value new column value
     */
    public function offsetSet($offset, $value) {
        if (!isset($this->_columns[$offset])) {
            $this->addColumn($offset);
        }
        $this->_columns[$offset]->setValue($value, TRUE);
    }

    /**
     * Check for column existence
     * @param string $offset column name
     * @return bool column eixsts
     */
    public function offsetExists($offset) {
        return isset($this->_columns[$offset]);
    }

    /*
     * This only unsets the column in the container, do delete use the
     * PandraColumn->delete() function
     * @return void
    */
    public function offsetUnset($offset) {
        unset($this->_columns[$offset]);
    }

    /**
     * Get column value by column name
     * @param string $offset column name
     * @return mixed column value
     */
    public function offsetGet($offset) {
        return $this->getColumn($offset)->value;
    }

    /**
     * retrieve the last error for this container
     * @return <type>
     */
    public function lastError() {
        if (!empty($this->errors)) {
            return $this->errors[0];
        }
        return NULL;
    }

    /**
     * Define a new column, type definition and callback
     * @param string $colName column name
     * @param array $typeDef validator type definitions
     * @param string $callbackOnSave callback function pre-save
     * @return PandraColumn reference to created column
     */
    public function addColumn($colName, $typeDef = array(), $callbackOnSave = NULL) {
        if (!array_key_exists($colName, $this->_columns)) {
            $this->_columns[$colName] = new PandraColumn($colName, $this, $typeDef);
        }

        // pre-save callback
        if (!empty($callbackOnSave)) $this->_columns[$colName]->callback = $callbackOnSave;

        return $this->getColumn($colName);
    }

    /**
     * Get reference to named PandraColumn
     * @param string $colName column name
     * @return PandraColumn
     */
    public function getColumn($colName) {
        if (array_key_exists($colName, $this->_columns)) {
            return $this->_columns[$colName];
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
        return (array_key_exists($colName, $this->_columns) && $this->_columns[$colName]->setValue($value, $validate));
    }

    /**
     * Get a list of columns in the container
     * @return array list of column names
     */
    public function listColumns() {
        return array_keys($this->_columns);
    }

    /**
     * 
     * @return array cassandra_Column objects
     */

    /**
     * Gets complete slice of Thrift cassandra_Column objects for keyID
     * @param mixed $keyID key id for row
     * @param int $consistencyLevel cassandra consistency level
     * @return cassandra_Cassandra_get_slice_result
     */
    public function getRawSlice($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        $this->keyID = $keyID;

        $this->_parentCF->checkCFState();

        $client = Pandra::getClient();

        // build the column path
        $columnParent = new cassandra_ColumnParent();
        $columnParent->column_family = $this->name;
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
        foreach  ($this->_columns as &$cObj) {
            $cObj->reset();
        }
    }

    /**
     * Populates container object (ColumnFamily, ColumnFamilySuper or SuperColumn)
     * @param mixed $data associative array or json string of key => values.
     * @return bool column values set without error
     */
    public function populate($data) {
        if (is_string($data)) {
            $data = json_decode($data, TRUE);
        }

        if (is_array($data)) {
            foreach ($data as $key => $value) {
                if (array_key_exists($key, $this->_columns)) {
                    $this->_columns[$key]->setValue($value);
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
    protected function _gsMutable(&$colName) {
        $colName = preg_replace("/^".self::_columnNamePrefix."/", "", strtolower($colName));
        return array_key_exists($colName, $this->_columns);
    }

    /**
     * Magic getter
     * @param string $colName field name to get
     * @return string value
     */
    protected function __get($colName) {
        if ($this->_gsMutable($colName)) {
            return $this->_columns[$colName]->value;
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
        if ($this->_gsMutable($colName)) {
            $this->setColumn($colName, $value);
        }
    }

    /**
     * Helper to determine if containr is a standard or super type
     * @return int PANDRA_SUPER or PANDRA_STANDARD
     */
    public function getType() {
        return constant(get_class($this)."::TYPE");
    }
}