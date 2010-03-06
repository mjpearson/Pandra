<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * Column Container abstract for (Super) Column Families and Super Columns, which contain our
 * working columns
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */

/**
 * @abstract
 */
abstract class PandraColumnContainer implements ArrayAccess {

    /* @var this column families name (table name) */
    protected $_name = NULL;

    protected $_keyID = NULL;

    protected $_keySpace = NULL;

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
     * CF constructor, calls init()
     * @param string $keyID row key id
     * @param string $keySpace Cassandra Keyspace
     * @param string $columnFamilyName  column family name
     */
    public function __construct($keyID = NULL, $keySpace = NULL, $name = NULL) {
        if ($keyID !== NULL) $this->setKeyID($keyID);
        if ($keySpace !== NULL) $this->setKeySpace($keySpace);
        if ($name !== NULL) $this->setName($name);
        $this->init();
    }

    /**
     * init is is always called by the constructor.  Child classes can implement
     * constructor logic, schemas, defaults validators etc. via init()
     * @return void
     */
    public function init() {        
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

    public function setKeySpace($keySpace) {
        $this->_keySpace = $keySpace;
    }

    public function getKeySpace() {
        return $this->_keySpace;
    }

    public function setKeyID($keyID) {
        $this->_keyID = $keyID;
    }

    public function getKeyID() {
        return $this->_keyID;
    }

    public function pathOK($keyID = NULL) {
        $ok = ( ($keyID !== NULL || $this->getKeyID() !== NULL) && $this->getKeySpace() !== NULL && $this->getName() !== NULL);
        if (!$ok) $this->registerError('Required field (Keyspace, ColumnFamily or KeyID) not present');
        return $ok;
    }

    /**
     * mutator, modified
     * @param bool $modified
     */
    protected function setModified($modified = TRUE) {
        $this->_modified = $modified;
    }

    /**
     * marks the container and subcolumns (or subcontainers) for deletion
     * operation cascades to columns
     * @return void
     */
    public function delete() {
        $this->setDelete(TRUE);
        foreach ($this->_columns as &$column) {
            $column->delete();
        }
    }

    /**
     * mutator, marks this column for deletion and sets modified
     * @param bool $delete
     */
    protected function setDelete($delete) {
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

    public function destroyErrors() {
        unset($this->errors);
        $this->errors = array();
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

    /**
     * This only unsets the column in the container, to delete use the
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
    public function offsetGet($columnName) {
        if ($columnName instanceof PandraClause) {
            return $this->getColumn($columnName);
        }
        return $this->__get($columnName);
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
     * @param string $columnName column name
     * @param array $typeDef validator type definitions
     * @param string $callbackOnSave callback function pre-save
     * @return PandraColumn reference to created column
     */
    public function addColumn($columnName, $typeDef = array(), $callbackOnSave = NULL) {
        if (!array_key_exists($columnName, $this->_columns)) {
            $this->_columns[$columnName] = new PandraColumn($columnName, $typeDef, $this);
        }

        // pre-save callback
        if (!empty($callbackOnSave)) $this->_columns[$columnName]->callback = $callbackOnSave;

        return $this->getColumn($columnName);
    }

    /**
     * Get reference to named PandraColumn
     * @param string $columnName column name
     * @return PandraColumn
     */
    public function getColumn($columnMatch) {

        // Extract matching named columns based on clause
        if ($columnMatch instanceof PandraClause) {

            $container = new PandraQuery();

            $matches = array();

            foreach ($this->_columns as $columnName => &$column) {
                if ($columnMatch->match($columnName)) {
                    $container->setColumn($columnName, $column);
                }
            }
            return $container;
        }

        if ($this->_gsMutable($columnMatch)) {
            return $this->_columns[$columnMatch];
        }
        return NULL;
    }

    public function getColumns() {
        return $this->_columns;
    }

    public function getColumnNames() {
        return array_keys($this->_columns);
    }

    /**
     * Sets a columns value for this slice
     * @param string $columnName Column name to set
     * @param string $value New value for column
     * @param bool $validate opt validate from typeDef (default TRUE)
     * @return bool column set ok
     */
    public function setColumn($columnName, $value, $validate = TRUE) {
        if (is_object($value)) {
            $this->_columns[$columnName] = $value;
            return TRUE;
        }

        return (array_key_exists($columnName, $this->_columns) && $this->_columns[$columnName]->setValue($value, $validate));
    }

    /**
     * Get a list of columns in the container
     * @return array list of column names
     */
    public function listColumns() {
        return array_keys($this->_columns);
    }

    /**
     * unmarks container and subcolumns (or subcontainers) for deletion
     * cascades to columns, unsets modified flag
     */
    public function reset() {
        $this->setDelete(FALSE);
        $this->setModified(FALSE);
        $cReset = FALSE;
        foreach ($this->_columns as &$column) {
            $cReset = $column->reset();
            if ($cReset == FALSE) break;
        }

        return (!$this->_delete && !$this->_modified && $cReset);
    }

    /**
     * removes a column from the container (does not delete from Cassandra)
     * @param string $columnName column name
     */
    public function destroyColumns($columnName = NULL) {
        if ($columnName === NULL) {
            if (array_key_exists($columnName, $this->_columns)) {
                unset($this->_columns[$columnName]);
            }
        } else {
            unset($this->_columns);
            $this->_columns = array();
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
    public function setAutoCreate($autoCreate) {
        $this->_autoCreate = $autoCreate;
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
            
            // Check depth, take first few keys as keyspace/columnfamily/key
            foreach ($data as $idx => $colValue) {
                if ($colValue instanceof cassandra_Column) {
                    if ($this->getAutoCreate($colAutoCreate) || array_key_exists($colValue->name, $this->_columns)) {
                        $this->_columns[$colValue->name] = PandraColumn::cast($colValue, $this);
                    }

                    // circular dependency?
                } elseif ($colValue instanceof cassandra_ColumnOrSuperColumn && !empty($colValue->column)) {
                    if ($this->getAutoCreate($colAutoCreate) || array_key_exists($colValue->column->name, $this->_columns)) {
                        $this->_columns[$colValue->column->name] = PandraColumn::cast($colValue->column, $this);
                    }
                } else {
                    $colExists = array_key_exists($idx, $this->_columns);
                    // Create a new named column object
                    if ($this->getAutoCreate($colAutoCreate) && !array_key_exists($idx, $this->_columns)) {
                        $this->addColumn($idx);
                    }

                    // Set Value
                    if (array_key_exists($idx, $this->_columns)) {
                        if ($this->_columns[$idx] instanceof PandraColumn) {
                            $this->_columns[$idx]->setValue($colValue);
                        }
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
     * @param string $columnName field name to check
     * @return bool field exists
     */
    protected function _gsMutable(&$columnName) {
        $columnName = preg_replace("/^".constant(get_class($this).'::_columnNamePrefix')."/", "", $columnName);
        return array_key_exists($columnName, $this->_columns);
    }

    /**
     * Magic getter
     * @param string $columnName field name to get
     * @return string value
     */
    public function __get($columnName) {
        if ($this->_gsMutable($columnName)) {

            if ($this->_columns[$columnName] instanceof PandraColumn) {
                return $this->_columns[$columnName]->value;

            } else if ($this->_columns[$columnName] instanceof PandraColumnContainer) {
                return $this->_columns[$columnName];
            }
        }
        return NULL;
    }

    /**
     * Magic setter
     * @todo propogate an exception for setcolumn if it returns false.  magic __set's are void return type
     * @param string $columnName field name to set
     * @param string $value  value to set for field
     * @return bool field set ok
     */
    public function __set($columnName, $value) {
        if (!$this->_gsMutable($columnName) && $this->getAutoCreate()) {
            $this->addColumn($columnName);
        }

        if (!$this->setColumn($columnName, $value)) {
            throw new RuntimeException($columnName.' set but does not exist in container');
        }
    }

    protected function setLoaded($loaded) {
        $this->_loaded = $loaded;
    }

    public function isLoaded() {
        return $this->_loaded;
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
     * @return bool Column Family is marked for deletion
     */
    public function isDeleted() {
        return ($this->_modified && $this->_delete);
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

    public function toJSON($keyPath = FALSE) {
        return json_encode($this->toArray($keyPath));
    }

    public function toArray($keyPath = FALSE) {
        $retArr = array();
        foreach ($this->_columns as $column) {
            if ($column instanceof PandraColumn) {
                $retArr[$column->name] = $column->value;
            } else {
                // keyspace/CF/key/{column or supercolumn}
                if ($keyPath) {
                    $retArr[$this->getKeySpace()][$this->getName()][$this->keyID][$column->getName()] = $column->toArray();
                } else {
                    $retArr[$column->getName()] = $column->toArray($keyPath);
                }

            }
        }

        return $retArr;
    }
}
?>