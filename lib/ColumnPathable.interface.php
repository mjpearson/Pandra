<?php
/**
 * PandraColumnPathable
 *
 * Column and Container child objects are bound to this interface
 *
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 *
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */

/**
 * @abstract
 */
interface PandraColumnPathable {

    /**
     * Loads a container column path by keyid
     * @param string $keyID optional row key
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID = NULL, $consistencyLevel = NULL);

    /**
     * Save this container column path and any modified columns to Cassandra
     * @param cassandra_ColumnPath $columnPath
     * @param int $consistencyLevel Cassandra consistency level
     * @return bool save ok
     */
    public function save($consistencyLevel = NULL);

    /**
     * Local reset method, handling delete/modified flags
     */
    public function reset();

    /**
     * Delete method
     */
    public function delete();

    /**
     * Modified accessor
     * @return bool child is marked as modified
     */
    public function isModified();

    /**
     * Deleted accessor
     * @return bool child is marked for deletion
     */
    public function isDeleted();

    /**
     * Child name accessor
     * @return string child name
     */
    public function getName();

    /**
     * keyID mutator
     * @param string $keyID row key id
     */
    public function setKeyID($keyID);

    /**
     * keyID accessor if local member has not been set, attempts to return the set parents attribute instead
     * @return string
     */
    public function getKeyID();

    /**
     * keySpace mutator
     * @param string $keySpace keyspace name
     */
    public function setKeySpace($keySpace);

    /**
     * keySpace accessor if local member has not been set, attempts to return the set parents attribute instead
     * @return string
     */
    public function getKeySpace();

    /**
     * Checks we have a bare minimum attributes on the entity, to perform a columnpath search
     * @param string $keyID optional overriding row key
     * @return bool columnpath looks ok
     */
    public function pathOK($keyID = NULL);

    /**
     * Creates an error entry in this column and propogate to parent
     * @param string $errorStr error string
     */
    public function registerError($errorStr);

    /**
     * Grabs all errors for the column instance
     * @return array all errors
     */
    public function getErrors();

    /**
     * Grabs the last logged error
     * @return string last error message
     */
    public function getLastError();

    /**
     * Destroys all errors in this container, and its children
     * @param bool $childPropogate optional propogate destroy to children (default TRUE)
     */
    public function destroyErrors();

}
?>