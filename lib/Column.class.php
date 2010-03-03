<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @link http://www.phpgrease.net/projects/pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */
class PandraColumn extends cassandra_Column {

    /* @var array validator type definitions for this colun */
    public $typeDef = array();

    /* @var string last processing error */
    public $errors = array();

    /* @var string callback function for this column pre-save */
    private $_callback = NULL;

    /* @var bool column value has been modified since load() or init */
    private $_modified = FALSE;

    /* @var $delete column is marked for deletion */
    private $_delete = FALSE;

    /* @var PandraColumnFamily column family parent reference */
    private $_parent = NULL;

    /* @var string row key id */
    private $_keyID = NULL;

    /* @var string column keyspace */
    private $_keySpace = NULL;

    /* @var string column family name */
    private $_columnFamilyName = NULL;

    /* @var string super column name */
    private $_superColumnName = NULL;

    // ----------------- CONSTRUCTOR AND PARENT BINDING

    /**
     * Column constructor (extends cassandra_Column)
     * @param string $name Column name
     * @param PandraColumnContainer $parent parent column family (standard or super), or supercolumn
     * @param array $typeDef validator type definitions
     */
    public function __construct($name, $typeDef = array(), PandraColumnContainer $parent = NULL, $callback = NULL) {
        parent::__construct(array('name' => $name));

        if ($parent !== NULL) $this->setParent($parent);

        if ($callback !== NULL) $this->setCallback($callback);

        $this->typeDef = $typeDef;
    }

    /**
     * Binds a ColumnFamily or SuperColumn as parent
     * @param PandraColumnContainer $parent
     */
    public function setParent(PandraColumnContainer $parent) {
        if (($parent instanceof PandraColumnFamily || $parent instanceof PandraSuperColumn) && !($parent instanceof PandraSuperColumnFamily)) {
            $this->_parent = $parent;
        } else {
            throw new RuntimeException('Column Family or Super Column parent expected, received : '.get_class($parent));
        }
    }

    /**
     * Gets the current working parent column family
     * @return <type>
     */
    public function getParent() {
        return $this->_parent;
    }

    // ----------------- MUTATORS AND ACCESSORS

    /**
     * Binds a timestamp to the column, defaults to current time if no override defined
     * @param int $time new time stamp
     * @return int new timestamp
     */
    public function bindTime($time = NULL) {
        $this->timestamp = ($time === NULL) ? PandraCore::getTime() : intval($time);
        $this->setModified();
        return $this->timestamp;
    }

    /**
     * Sets the value of the column
     * @param mixed $value new value
     * @param bool $validate validate the value, if typeDef is set
     * @return bool column set ok (check errors for details)
     */
    public function setValue($value, $validate = TRUE) {
        if ($validate && !empty($this->typeDef)) {
            if (!PandraValidator::check($value, $this->name, $this->typeDef, $this->errors)) {
                if ($this->_parent !== NULL) {
                    $this->_parent->registerError($this->errors[0]);
                }
                return FALSE;
            }
        }

        if ($this->value == $value) return TRUE;

        $this->value = $value;
        $this->setModified();
        return TRUE;

    }

    /**
     * Value accessor (cassandra_Column->value is public anyway, suggest using this incase that changes)
     * @return string value
     */
    public function getValue() {
        return $this->value;
    }

    /**
     * Callback mutator, throws a RuntimeException if function does not exist
     * @param string $cbFunc callback function name
     */
    public function setCallback($cbFunc) {
        if (!function_exists($cbFunc)) {
            throw new RuntimeException("Function $cbFunc could not be found");
        } else {
            $this->_callback = $cbFunc;
        }
    }

    /**
     * Callback accessor
     * @return string pre-save callback function name
     */
    public function getCallback() {
        return $this->_callback;
    }

    /**
     * returns the callback function value for this->value
     * @return mixed result of callback eval
     */
    public function callbackvalue() {
        if ($this->_callback === NULL) {
            return $this->value;
        }
        return call_user_func($this->_callback, $this->value);
    }

    /**
     * keyID mutator
     */
    public function setKeyID($keyID) {
        $this->_keyID = $keyID;
    }

    /**
     * keyID accessor if local member has not been set, attempts to return the set parents attribute instead
     * @return string
     */
    public function getKeyID() {
        $parent = $this->getParent();
        if ($this->_keyID === NULL && $parent !== NULL) {
            return $parent->getKeyID();
        }
        return $this->_keyID;
    }

    /**
     * keySpace mutator
     */
    public function setKeySpace($keySpace) {
        $this->_keySpace = $keySpace;
    }

    /**
     * keySpace accessor if local member has not been set, attempts to return the set parents attribute instead
     * @return string
     */
    public function getKeySpace() {
        $parent = $this->getParent();
        if ($this->_keySpace === NULL && $parent !== NULL) {
            return $parent->getKeySpace();
        }
        return $this->_keySpace;
    }

    /**
     * columnFamilyName mutator
     */
    public function setColumnFamilyName($columnFamilyName) {
        $this->_columnFamilyName = $columnFamilyName;
    }

    /**
     * columnFamilyName accessor if local member has not been set, attempts to return the set parents attribute instead
     * @return string
     */
    public function getColumnFamilyName() {
        $parent = $this->getParent();
        if ($this->_columnFamilyName === NULL) {
            if ($parent instanceof PandraSuperColumn) {
                return $parent->getParent()->getName();
            } elseif ($parent instanceof PandraColumnFamily) {
                return $parent->getName();
            }
        }
        return $this->_columnFamilyName;
    }

    /**
     * superColumnName mutator
     */
    public function setSuperColumnName($superColumnName) {
        $this->_superColumnName = $superColumnName;
    }

    /**
     * superColumnName accessor if local member has not been set, attempts to return the set parents attribute instead
     * @return string
     */
    public function getSuperColumnName() {
        $parent = $this->getParent();
        if ($this->_superColumnName === NULL && $parent instanceof PandraSuperColumn) {
            return $parent->getParent()->getName();
        }
        return $this->_superColumnName;
    }

    // ----------------- Saves and Loads

    /**
     * Casts from a cassandra_ColumnOrSuperColumn->column or cassandra_Column types, to PandraColumn
     * @param cassandra_Column $object source objct
     * @param PandraColumnContainer $parent parent container
     * @return PandraColumn new column object or NULL on empty cassandra_ColumnOrSuperColumn->column
     */
    static public function cast($object, PandraColumnContainer $parent = NULL) {

        if ($object instanceof cassandra_ColumnOrSuperColumn) {
            if (!empty($object->column->name)) {
                $object = $object->column;
            } else {
                return NULL;
            }

        } elseif (!($object instanceof cassandra_Column)) {
            throw new RuntimeException('Cast expected cassandra_Column[OrSuperColumn], recieved '.get_class($object));
        }

        $newObj = new PandraColumn($object->name);

        if ($parent !== NULL) $newObj->setParent($parent);

        $newObj->setValue($object->value);
        $newObj->bindTime($object->timestamp);

        return $newObj;
    }

    public function load($consistencyLevel = NULL) {
        // @todo
    }

    /**
     * Saves this individual column path, where a parent has been set (setParent()) keyid, keyspace, columnfamily or supercolumn
     * will be inherited for the save.
     * @return bool save ok
     */
    public function save($consistencyLevel = NULL) {

        if (!$this->isModified()) {
            $this->registerError("Column $name is not modified");
            return FALSE;
        }

        // Build the column path for modifying this individual column
        $columnPath = new cassandra_ColumnPath();
        $columnPath->column_family = $this->getColumnFamilyName();
        $columnPath->super_column = $this->getSuperColumnName();
        $columnPath->column = $this->name;

        $ok = FALSE;

        if ($this->_delete) {
            $ok = PandraCore::deleteColumnPath($this->getKeySpace(), $this->getKeyID(), $columnPath, $this->bindTime(), PandraCore::getConsistency($consistencyLevel));
        } else {
            $ok = PandraCore::saveColumnPath($this->getKeySpace(), $this->getKeyID(), $columnPath, $this->callbackvalue(), $this->bindTime(), PandraCore::getConsistency($consistencyLevel));
        }

        if (!$ok) {
            if (empty(PandraCore::$lastError)) {
                $errorStr = 'Unknown Error';
            } else {
                $errorStr = PandraCore::$lastError;
            }

            $this->registerError($errorStr);
        }

        if ($ok) $this->reset();
        return $ok;
    }

    // ----------------- ERROR HANDLING

    /**
     * Creates an error entry in this column and propogate to parent
     * @param string $errorStr error string
     */
    public function registerError($errorStr) {
        if (!empty($errorStr)) {
            array_push($this->errors, $errorStr);
            if ($this->_parent !== NULL) $this->_parent->registerError($errorStr);
        }
    }

    /**
     * Grabs all errors for the column instance
     * @return array all errors
     */
    public function getErrors() {
        return $this->errors;
    }

    /**
     * Grabs the last logged error
     * @return string last error message
     */
    public function getLastError() {
        if (count($this->errors)) {
            return $this->errors[0];
        }
        return NULL;
    }

    // ----------------- MODIFY/DELETE MUTATORS AND ACCESSORS

    /**
     * Removes any modified or delete flags, (does not revert values)
     */
    public function reset() {
        $this->_modified = FALSE;
        $this->_delete = FALSE;
        return TRUE;
    }

    /**
     * mutator, marks this column for deletion and sets modified
     */
    public function delete() {
        $this->_delete = TRUE;
        $this->_modified = TRUE;
    }

    public function setDelete($delete) {
        $this->_delete = $delete;
    }

    public function getDelete() {
        return $this->_delete;
    }

    /**
     * Delete accessor
     * @return bool Column is marked for deletion
     */
    public function isDeleted() {
        return ($this->_delete && $this->_modified);
    }

    public function isModified() {
        return $this->_modified;
    }

    /**
     * Modified mutator
     * @param bool $modified column is modified
     */
    public function setModified($modified = TRUE) {
        $this->_modified = $modified;
    }

    /**
     * Modified accessor
     * @return bool column has been modified since instance construct/load
     */
    public function getModified() {
        return $this->_modified;
    }
}
?>