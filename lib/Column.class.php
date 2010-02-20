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
    public function __construct($name, $parentCF = NULL, $typeDef = array()) {
        parent::__construct(array('name' => $name));
        if ($parentCF instanceof PandraColumnContainer) {
            $this->setParentCF($parentCF);
        }

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
     * @param int $time new time stamp, microtime assumed (optional)
     * @return int new timestamp
     */
    public function bindTime($time = NULL) {
        //$this->timestamp = ($time === NULL) ? round(microtime(true) * 1000, 3) : intval($time);
        $this->timestamp = ($time === NULL) ? time() : intval($time);
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
                if ($this->_parentCF instanceof PandraColumnContainer) {
                    $this->_parentCF->errors[] = $this->errors[0];
                }
                return FALSE;
            }
        }

        $this->value = $value;
        $this->setModified();
        return TRUE;
    }

    /**
     * returns the callback function value for this->value
     * @return mixed result of callback eval
     */
    public function callbackvalue() {
        return call_user_func($this->callback, $this->value);
    }

    /**
     * Casts from a cassandra_Column type, to PandraColumn
     * @param cassandra_Column $object source objct
     * @param PandraColumnFamily $parentCF parent container
     * @return PandraColumn new column object
     */
    static public function cast(cassandra_Column $object, $parentCF = NULL) {
        $newObj = new PandraColumn($object->name, ($parentCF === NULL) ? $this->_parentCF : $parentCF);
        $newObj->value = $object->value;
        $newObj->timestamp = $object->timestamp;
        return $newObj;
    }

    /**
     * Saves this individual column path
     * @param string $keyID row key
     * @param sring $keySpace key space
     * @param string $columnFamily column family name
     * @param int $consistencyLevel cassandra save consistency level
     * @return bool save ok
     */
    public function save($keyID, $keySpace, $columnFamily, $consistencyLevel = NULL) {

        if (!$this->isModified()) return TRUE;

        // Build the column path for modifying this individual column
        $columnPath = new cassandra_ColumnPath();
        $columnPath->column_family = $columnFamily;
        $columnPath->column = $this->name;

        $ok = FALSE;

        if ($this->_delete) {
            $ok = PandraCore::deleteColumnPath($keySpace, $keyID, $columnPath, $this->bindTime(), PandraCore::getConsistency($consistencyLevel));
        } else {
            $ok = PandraCore::saveColumnPath($keySpace, $keyID, $columnPath, ($this->callback === NULL) ? $this->value : $this->callbackvalue(), $this->bindTime(), PandraCore::getConsistency($consistencyLevel));
        }

        if (!$ok) {
            if (empty(PandraCore::$errors)) {
                $errorStr = 'Unkown Error';
            } else {
                $errorStr = PandraCore::$errors;
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
            if ($this->_parentCF instanceof PandraColumnContainer) $this->_parentCF->registerError($errorStr);
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
    }

    /**
     * mutator, marks this column for deletion and sets modified
     */
    public function delete() {
        $this->_delete = TRUE;
        $this->setModified();
    }

    /**
     * Delete accessor
     * @return bool Column is marked for deletion
     */
    public function isDeleted() {
        return ($this->_delete && $this->_modified);
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
    public function isModified() {
        return $this->_modified;
    }
}
?>