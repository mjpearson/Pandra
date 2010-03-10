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
interface PandraContainerSavable {

    /**
     * Loads a container column path by keyid
     * @param string $keyID optional row key
     * @param bool $colAutoCreate create columns in the object instance which have not been defined
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID = NULL, $colAutoCreate = NULL, $consistencyLevel = NULL);

    /**
     * Save this container column path and any modified columns to Cassandra
     * @param cassandra_ColumnPath $columnPath
     * @param int $consistencyLevel Cassandra consistency level
     * @return bool save ok
     */
    public function save($consistencyLevel = NULL);

}
?>