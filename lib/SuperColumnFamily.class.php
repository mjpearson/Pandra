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
class PandraSuperColumnFamily extends PandraColumnFamily {

    /* @var string magic get/set prefix for Super Columns */
    const _columnNamePrefix = 'super_';

    /**
     * Helper function to add a Super Column instance to this Super Column Family
     * @param PandraSuperColumn $scObj
     * @return PandraSuperColumn
     */
    public function addSuper(PandraSuperColumn $scObj) {

        $superName = $scObj->getName();

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

    public function save($consistencyLevel = NULL) {

        if (!$this->isModified()) return FALSE;

        $ok = FALSE;

        // Deletes the entire columnfamily by key
        if ($this->isDeleted()) {
            $columnPath = new cassandra_ColumnPath();
            $columnPath->column_family = $this->getName();

            $ok = Pandra::deleteColumnPath($this->getKeySpace(), $this->keyID, $columnPath, time(), Pandra::getConsistency($consistencyLevel));
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
    public function load($keyID, $colAutoCreate = NULL, $consistencyLevel = NULL) {

        $this->_loaded = FALSE;

        $result = Pandra::getCFSlice($keyID, $this->getKeySpace(), $this->getName(), NULL, Pandra::getConsistency($consistencyLevel));

        if ($result !== NULL) {
            $this->init();
            foreach ($result as $superColumn) {
                $sc = $superColumn->super_column;

                // @todo Should at least 1 successful super load really indicate a successful load state?
                $this->_loaded = $this->addSuper(new PandraSuperColumn($sc->name))->populate($sc->columns, $this->getAutoCreate($colAutoCreate));
            }
            if ($this->_loaded) $this->setKeyID($keyID);

        } else {
            $this->registerError(Pandra::$lastError);
        }

        return $this->_loaded;
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

                // Allow named SuperColumns to be populated into this CF
                if ($colValue instanceof PandraSuperColumn) {
                    if ($this->getAutoCreate($colAutoCreate) || array_key_exists($idx, $this->_columns)) {
                        $this->_columns[$idx] = $colValue;
                    }

                } else {
                    if ($this->getAutoCreate($colAutoCreate) || array_key_exists($idx, $this->_columns)) {
                        $this->addSuper(new PandraSuperColumn($idx), $this);
                        $this->getSuper($idx)->populate($colValue);
                    }
                }
            }
        } else {
            return FALSE;
        }

        return empty($this->errors);
    }
}
?>