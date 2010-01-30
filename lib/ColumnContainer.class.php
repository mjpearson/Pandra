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
abstract class PandraColumnContainer extends ArrayObject {

    /* @var this column families name (table name) */
    protected $_name = NULL;

    /* @var string magic set/get prefixes for Columns */
    const _columnNamePrefix = 'column_';	// magic __get/__set column prefix in column famliy

    /* @var array container for column objects, indexed to field name */
    protected $_columns = array();

    /* @var array complete list of errors for this object instance */
    public $errors = array();

    /* @var bool columnfamily marked for deletion */
    protected $_delete = FALSE;

    /* @var bool container (columns) have been modified */
    protected $_modified = FALSE;

    /* @var bool container columns have been loaded from Cassandra */
    protected $_loaded = FALSE;

    /* @var bool auto create columns/containers loaded from Cassandra which do not exist in the local container */
    protected $_autoCreate = TRUE;

    /**
     * Constructor, calls init()
     */
    public function __construct() {
        $this->init();
    }

    /**
     * accessor, container name
     * @return string container name
     */
    public function getName() {
        return $this->_name;
    }

    /**
     * mutator, container name
     * @param string $name new name
     */
    public function setName($name) {
        $this->_name = $name;

    }

    /**
     * init is is always called by the constructor.  Child classes can implement
     * constructor logic, schemas, defaults validators etc. here
     * @return void
     */
    public function init() {
    }

    /**
     * mutator, modified
     * @param bool $modified
     */
    protected function setModified($modified = TRUE) {
        $this->_modified = $modified;
    }

    /**
     * mutator, marks this column for deletion and sets modified
     * @param bool $delete
     */
    protected function setDelete($delete) {
        $this->setModified($delete);
        $this->_delete = $delete;
    }

    /**
     * accessor, delete
     * @return bool container is marked for deletion
     */
    public function getDelete() {
        return $this->_delete;
    }

    /**
     * Creates an error entry in this column and propogate to parent
     * @param string $errorStr error string
     */
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
    }

    /**
     * Sets a columns value for this slice
     * @param string $colName Column name to set
     * @param string $value New value for column
     * @param bool $validate opt validate from typeDef (default TRUE)
     * @return bool column set ok
     */
    public function setColumn($colName, $value, $validate = TRUE) {
        if (is_object($value)) {
            $this->_columns[$colName] = $value;
            return TRUE;
        }

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
     * removes a column from the container (does not delete from Cassandra)
     * @param string $colName column name
     */
    public function destroyColumn($colName) {
        if (array_key_exists($colName, $this->_columns)) {
            unset($this->_columns[$colName]);
        }
    }

    /**
     * Get working autocreate mode (either this set autocreate or overriden)
     * @param bool $override
     * @return bool working autocreate mode
     */
    public function getAutoCreate($override = NULL) {
        $autoCreate = $this->_autoCreate;

        if ($override !== NULL) {
            $autoCreate = $override;
        }

        return $autoCreate;
    }

    /**
     * autoCreate mutator
     * @param bool $autoCreate new mode
     */
    public function setAutoCreate(bool $autoCreate) {
        $this->$_autoCreate = $autoCreate;
    }

    /**
     * Populates container object (ColumnFamily, ColumnFamilySuper or SuperColumn)
     * @param mixed $data associative string array, array of cassandra_Column's or JSON string of key => values.
     * @return bool column values set without error
     */
    public function populate($data, $colAutoCreate = NULL) {
        if (is_string($data)) {
            $data = json_decode($data, TRUE);
        }

        if (is_array($data) && count($data)) {
            foreach ($data as $idx => $colValue) {

                if ($colValue instanceof cassandra_Column) {
                    if ($this->getAutoCreate($colAutoCreate) || array_key_exists($colValue->name, $this->_columns)) {
                        $this->_columns[$colValue->name] = PandraColumn::cast($colValue, $this);
                    }

                } else {
                    $colExists = array_key_exists($idx, $this->_columns);
                    // Create a new named column object
                    if ($this->getAutoCreate($colAutoCreate) && !array_key_exists($idx, $this->_columns)) {
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
    public function __get($colName) {
        if ($this->_gsMutable($colName)) {

            if ($this->_columns[$colName] instanceof PandraColumn) {
                return $this->_columns[$colName]->value;

            } else if ($this->_columns[$colName] instanceof PandraColumnContainer) {
                return $this->_columns[$colName];
            }
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
        if (!$this->_gsMutable($colName)) {
            $this->addColumn($colName);
        }

        if (!$this->setColumn($colName, $value)) {
            throw new RuntimeException($colName.' set but does not exist in container');
        }
    }

    /**
     * accessor, checks if container has been explicitly modified, or it sub columns
     * @return <type>
     */
    public function isModified() {
        foreach ($this->_columns as $column) {
            if ($column->isModified()) return TRUE;
        }
        return $this->_modified;
    }

    /**
     * Binds current time to all modified columns
     * @param int $timeOverride optional timestamp, will default to time() if NULL
     */
    public function bindTimeModifiedColumns($timeOverride = NULL) {
        foreach ($this->_columns as &$cObj) {
            if ($cObj->isModified()) {
                $cObj->bindTime($timeOverride);
            }
        }
    }

    /**
     * Returns all columns which have been modified
     * @return array array of  modified columns
     */
    public function getModifiedColumns() {
        $modColumns = array();
        foreach ($this->_columns as &$cObj) {
            if ($cObj->isModified()) $modColumns[] = &$cObj;
        }
        return $modColumns;
    }
}
?>