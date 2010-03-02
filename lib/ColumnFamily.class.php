<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * Container for Cassandra Column Families
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */
class PandraColumnFamily extends PandraColumnContainer {

    /**
     * Constructor, builds column structures
     */
    public function __construct($keyID = NULL, $keySpace = NULL, $columnFamilyName = NULL) {
        if ($keyID !== NULL) $this->setKeyID($keyID);
        if ($keySpace !== NULL) $this->setKeySpace($keySpace);
        if ($columnFamilyName !== NULL) $this->setName($columnFamilyName);
        parent::__construct();
    }

    /**
     * Loads an entire columnfamily by keyid
     * @param string $keyID row key
     * @param bool $colAutoCreate create columns in the object instance which have not been defined
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID = NULL, $colAutoCreate = NULL, $consistencyLevel = NULL) {

        if ($keyID === NULL) $keyID = $this->getKeyID();

        $ok = $this->pathOK($keyID);

        $this->setLoaded(FALSE);

        if ($ok) {

            $autoCreate = $this->getAutoCreate($colAutoCreate);

            // if autocreate is turned on, get everything
            if ($autoCreate) {
                $result = PandraCore::getCFSlice($this->getKeySpace(), $keyID, $this->getName(), NULL, PandraCore::getConsistency($consistencyLevel));
            } else {
                // otherwise by defined columns (slice query)
                $result = PandraCore::getCFSliceMulti($this->getKeySpace(), array($keyID), $this->getName(), NULL, array_keys($this->_columns), $consistencyLevel);
                $result = $result[$keyID];
            }

            if ($result !== NULL) {
                $this->init();
                $this->setLoaded($this->populate($result, $autoCreate));

                if ($this->isLoaded()) $this->setKeyID($keyID);
            } else {
                $this->registerError(PandraCore::$lastError);
            }
        }

        return ($ok && $this->isLoaded());
    }

    /**
     * Save this column family and any modified columns to Cassandra
     * @param cassandra_ColumnPath $columnPath
     * @param int $consistencyLevel Cassandra consistency level
     * @return bool save ok
     */
    public function save($consistencyLevel = NULL) {

        $ok = $this->pathOK();

        if ($ok) {
            if ($this->getDelete()) {

                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->getName();

                $ok = PandraCore::deleteColumnPath($this->getKeySpace(), $this->getKeyID(), $columnPath, NULL, PandraCore::getConsistency($consistencyLevel));
                if (!$ok) $this->registerError(PandraCore::$lastError);

            } else {
                // @todo have this use thrift batch_insert method in core
                //$this->bindTimeModifiedColumns();
                $modifiedColumns = $this->getModifiedColumns();
                foreach ($modifiedColumns as &$cObj) {
                    if (!$cObj->save(PandraCore::getConsistency($consistencyLevel))) {
                        $this->registerError($cObj->getLastError());
                        return FALSE;
                    }
                }
                $ok = TRUE;
            }
            if ($ok) $this->reset();
        }
        return $ok;
    }
}
?>
