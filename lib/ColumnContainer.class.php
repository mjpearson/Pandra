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
    protected $name = NULL;

    /* @var mixed keyID key for the working row */
    // @todo GET RID OF THIS
    private $keyID = NULL;

    /* @var string magic set/get prefixes for Columns */
    const _columnNamePrefix = 'column_';	// magic __get/__set column prefix in column famliy

    /* @var array container for column objects, indexed to field name */
    protected $_columns = array();

    /* @var int column family type (standard or super) */
    const TYPE = PANDRA_STANDARD;

    /* @var array complete list of errors for this object instance */
    public $errors = array();

    /* var bool columnfamily marked for deletion */
    private $_delete = FALSE;    

    private $_modified = FALSE;

    protected $_loaded = FALSE;

    public function __construct() {
        $this->constructColumns();
    }

    public function getName() {
        return $this->name;
    }

    public function setName($name) {
        $this->name = $name;

    }

    /**
     * constructFields builds column objects via addColumn/addSuper methods
     * when defining hard schemas.  It is called automatically by the constructor.
     * @return void
     */
    public function constructColumns() { }

    protected function setModified($modified) {
        $this->_modified = $modified;
    }

    protected function setDelete($delete) {
        $this->setModified(TRUE);
        $this->_delete = $delete;
    }

    protected function getDelete() {
        return $this->_delete;
    }

    public function registerError($errorStr) {
        if (empty($errorStr)) return;
        $this->errors[] = $errorStr;
    }

    /**
     * Sets value with a validator.  To skip validation, use explicit
     * PandraColumn->setValue($value, FALSE); or do not provide a typeDef
     * @param string $offset column name
     * @param mixed $value new column value
     */
    public function offsetSet($offset, $value) {
        $this->__set($offset, $value);
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
        return $this->__get($offset);
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
        if ($this->_gsMutable($colName)) {
            return $this->_columns[$colName];
        }
        return NULL;
        //return $this->__get($colName);
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
     * @param mixed $data associative string array, array of cassandra_Column's or JSON string of key => values.
     * @return bool column values set without error
     */
    public function populate($data, $colAutoCreate = PANDRA_DEFAULT_CREATE_MODE) {
        if (is_string($data)) {
            $data = json_decode($data, TRUE);
        }

        if (is_array($data) && count($data)) {
            foreach ($data as $idx => $colValue) {

                if ($value instanceof cassandra_Column) {                    
                    if ($colAutoCreate || array_key_exists($colValue->column->name, $this->_columns)) {
                        $this->_columns[$colValue->column->name] = PandraColumn::cast($colValue->column, $this);
                    }
                    
                } else {
                    $colExists = array_key_exists($idx, $this->_columns);
                    // Create a new named column object
                    if ($colAutoCreate && !array_key_exists($idx, $this->_columns)) {
                        $this->addColumn($idx);
                    }

                    // Set Value
                    if (array_key_exists($idx, $this->_columns)) {
                        $this->_columns[$idx]->setValue($colValue);
                    }
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
        $colName = preg_replace("/^".constant(get_class($this).'::_columnNamePrefix')."/", "", $colName);
        return array_key_exists($colName, $this->_columns);
    }

    /**
     * Magic getter
     * @param string $colName field name to get
     * @return string value
     */
    protected function __get($colName) {
        if ($this->_gsMutable($colName)) {

            if ($this->_columns[$colName] instanceof PandraColumnContainer && $this->_columns[$colName]->getType() == PANDRA_SUPER) {
                return $this->_columns[$colName];

            } else if ($this->_columns[$colName] instanceof PandraColumn) {
                return $this->_columns[$colName]->value;

            } else {
                return $this->_columns[$colName];
            }
        }
        //echo "$colName mutable <br>";
        return NULL;
    }

    /**
     * Magic setter
     * @todo propogate an exception for setcolumn if it returns false.  magic __set's are void return type
     * @param string $colName field name to set
     * @param string $value  value to set for field
     * @return bool field set ok
     */
    protected function __set($colName, $value) {
        if (!$this->_gsMutable($colName)) {
            $this->addColumn($colName);
        }
        $this->setColumn($colName, $value);
    }

    /**
     * Helper to determine if containr is a standard or super type
     * @return int PANDRA_SUPER or PANDRA_STANDARD
     */
    public function getType() {
        return constant(get_class($this)."::TYPE");
    }

    public function isModified() {
        foreach ($this->_columns as $column) {
            if ($column->isModified()) return TRUE;
        }
        return FALSE;
    }

    public function bindTimeModifiedColumns() {
        foreach ($this->_columns as &$cObj) {
            if ($cObj->isModified()) {
                $cObj->bindTime();
            }
        }
    }

    public function getModifiedColumns() {
        $modColumns = array();
        foreach ($this->_columns as &$cObj) {
            if ($cObj->isModified()) $modColumns[] = &$cObj;
        }
        return $modColumns;
    }
}
?>