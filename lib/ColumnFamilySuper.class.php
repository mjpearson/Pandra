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
abstract class PandraColumnFamilySuper extends PandraColumnFamily {

    /* @var int column family type (standard or super) */
    const TYPE = PANDRA_SUPER;

    /* @var string magic get/set prefix for Super Columns */
    const _columnNamePrefix = 'super_';

    /**
     * Helper function to add a Super Column instance to this Super Column Family
     * @param PandraSuperColumn $scObj
     * @return PandraSuperColumn
     */
    public function addSuper(PandraSuperColumn $scObj) {

        $superName = $scObj->name;

        $scObj->setParentCF($this);
        $this->_columns[$superName] = $scObj;

        return $this->getColumn($superName);
    }

    public function delete() {
        $this->setDelete(TRUE);
    }

    /**
     * Define a new named SuperColumn, anologous to ColumnFamily->addColumn
     * The only real difference between addColumn and addSuper in a SuperColumn
     * context, is addColumn will not overwrite the column with a new named instance
     * @param string $superName super column name
     * @return PandraSuperColumn reference to created column
     */
    public function addColumn($superName) {

        if (!array_key_exists($superName, $this->_columns)) {
            $newSuper = new PandraSuperColumn($superName);
            return $this->addSuper($newSuper);
        }

        return $this->getSuper($superName);
    }

    public function isModified() {
        foreach ($this->_columns as $superColumn) {
            if ($superColumn->isModified()) return TRUE;
        }
        return $this->_modified;
    }

    /**
     *
     * @param <type> $superName
     * @return <type>
     */
    public function getSuper($superName) {
        return $this->getColumn($superName);
    }

    public function save($consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        if (!$this->isModified()) return FALSE;

        $ok = FALSE;

        // Deletes the entire columnfamily by key
        if ($this->isDeleted()) {
            $columnPath = new cassandra_ColumnPath();
            $columnPath->column_family = $this->getName();

            $ok = Pandra::deleteColumnPath($this->getKeySpace(), $this->keyID, $columnPath, time(), $consistencyLevel);
            if (!$ok) $this->registerError(Pandra::$lastError);

        } else {
            foreach ($this->_columns as $colName => $superColumn) {
                $ok = $superColumn->save();
                if (!$ok) {
                    $this->registerError(Pandra::$lastError);
                    break;
                }
            }
        }

        return $ok;
    }

    /**
     * Loads an entire columnfamily by keyid
     * @param string $keyID row key
     * @param bool $colAutoCreate create columns in the object instance which have not been defined
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID, $colAutoCreate = PANDRA_DEFAULT_CREATE_MODE, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        $this->_loaded = FALSE;

        $result = Pandra::getCFSlice($keyID, $this->getKeySpace(), $this->getName(), NULL, $consistencyLevel);

        if ($result !== NULL) {
            $this->init();
            foreach ($result as $superColumn) {
                $sc = $superColumn->super_column;

                // @todo Should at least 1 successful super load really indicate a successful load state?
                $this->_loaded = $this->addSuper(new PandraSuperColumn($sc->name))->populate($sc->columns, PANDRA_DEFAULT_CREATE_MODE);
            }
            if ($this->_loaded) $this->setKeyID($keyID);

        } else {
            $this->registerError(Pandra::$lastError);
        }

        return $this->_loaded;
    }
}
?>