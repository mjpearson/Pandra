<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * Container classes are expected to implement their own save and load methods
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


}
?>