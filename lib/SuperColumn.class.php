<?php
/**
 * PandraSuperColumn
 *
 * SuperColumns are special kinds of Column Containers which can contain both
 * Columns and have a ColumnFamily parent (implements ContainerChild)
 *
 * 'super column family' => // Super Column Family
 *                      'my supercolumn' => {  // Super Column
 *                                 'column1',  // Column
 *                                 'column2',  // Column
 *                                 'column3'   // Column
 *                               },
 *                      'my supercolumn2' => { // Super Column
 *                                 'column4',  // Column
 *                                 'column5',  // Column
 *                                 'column6'   // Column
 *                               },
 *                      }
 *
 * PandraSuperColumn is a ColumnContainer child, and is both a ContainerChild itself,
 * and ColumnPathable
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.2
 * @package pandra
 */
class PandraSuperColumn extends PandraColumnContainer implements PandraContainerChild, PandraColumnPathable {

    /* @var PandraColumnFamily column family parent reference */
    private $_parent = NULL;

    /* @var string parent column family name, overrides parent */
    private $_columnFamilyName = NULL;

    /**
     * Supercolumn constructor
     * @param string $superName Super Column name
     * @param PandraSuperColumnFamily $parent
     */
    public function __construct($superName, PandraSuperColumnFamily $parent = NULL, $keyID = NULL, $keySpace = NULL, $name = NULL) {
        // SuperColumn name
        $this->setName($superName);

        // Reference parent ColumnFamilySuper
        if ($parent !== NULL) {
            $this->setParent($parent, !$parent->columnIn($superName));
        }
        parent::__construct($keyID, $keySpace, $name);
    }

    public function pathOK($keyID = NULL) {
        if ($this->_parent === NULL) {
            return $this->pathOK($keyID);
        }
        return $this->_parent->pathOK($keyID);
    }

    /**
     * Save all columns in this loaded columnfamily
     * @return void
     */
    public function save($consistencyLevel = NULL) {

        if (!$this->isModified()) return FALSE;

        $ok = $this->pathOK();

        if ($ok) {
            if ($this->getDelete()) {

                $columnPath = new cassandra_ColumnPath();
                $columnPath->column_family = $this->_parent->getName();
                $columnPath->super_column = $this->getName();
                $ok = PandraCore::deleteColumnPath($this->_parent->getKeySpace(),
                        $this->_parent->getKeyID(),
                        $columnPath,
                        NULL,
                        PandraCore::getConsistency($consistencyLevel));
            } else {

                $this->bindTimeModifiedColumns();
                $ok = PandraCore::saveSuperColumn(
                                    $this->_parent->getKeySpace(),
                                    $this->_parent->getKeyID(),
                                    array($this->_parent->getName()),
                                    array($this->getName() => $this->getModifiedColumns()),
                                    PandraCore::getConsistency($consistencyLevel));
            }

            if ($ok) {
                $this->reset();
            } else {
                $this->registerError(PandraCore::$lastError);
            }
        }

        return $ok;
    }

    /**
     * Loads a SuperColumn for key
     *
     * @param string $keyID optional row key
     * @param int $consistencyLevel cassandra consistency level
     * @return bool loaded OK
     */
    public function load($keyID = NULL, $consistencyLevel = NULL) {

        if ($keyID === NULL) $keyID = $this->getKeyID();

        $ok = $this->pathOK($keyID);

        $this->setLoaded(FALSE);

        if ($ok) {

            $autoCreate = $this->getAutoCreate();

            $predicate = new cassandra_SlicePredicate();

            // if autocreate is turned on, get everything
            if ($autoCreate) {

                $predicate->slice_range = new cassandra_SliceRange();
                $predicate->slice_range->start = '';
                $predicate->slice_range->finish = '';

                $result = PandraCore::getCFSlice(
                        $this->getKeySpace(),
                        $keyID,
                        new cassandra_ColumnParent(
                                array(
                                    'column_family' => $this->getColumnFamilyName(),
                                    'super_column' => $this->getName())),
                        $predicate,
                        PandraCore::getConsistency($consistencyLevel));

                // otherwise by defined columns (slice query)
            } else {

                $predicate->column_names = $this->getColumnNames();

                $result = PandraCore::getCFSliceMulti(
                        $this->getKeySpace(),
                        array($keyID),
                        $predicate,
                        new cassandra_ColumnParent(
                                array(
                                    'column_family' => $this->getColumnFamilyName(),
                                    'super_column' => $this->getName())),
                        PandraCore::getConsistency($consistencyLevel));

                $result = $result[$keyID];
            }

            if (!empty($result)) {
                $this->init();
                $this->setLoaded($this->populate($result, $autoCreate));
                if ($this->isLoaded()) $this->keyID = $keyID;
            } else {
                $this->registerError(PandraCore::$lastError);
            }
        }
        return ($ok && $this->isLoaded());
    }

    /**
     * Sets parent ColumnFamily or
     * @param PandraColumnContainer $parent SuperColumnFamily container object, or NULL
     */
    public function setParent($parent, $bindToParent = TRUE) {

        if (!($parent instanceof PandraSuperColumnFamily))
            throw new RuntimeException('Parent must be an instnace of PandraSuperColumnFamily');

        if ($bindToParent) $parent->addSuperColumnObj($this);

        // unbind existing parent
        $this->detach();

        $this->_parent = $parent;
    }

    /**
     * Gets the current working parent column family
     * @return <type>
     */
    public function getParent() {
        return $this->_parent;
    }

    /**
     * Nullifies a parent
     */
    public function nullParent($localDetach = TRUE) {
        if ($localDetach) $this->detach();
        $this->_parent = NULL;
    }

    /**
     * Calls parent unset for this column
     */
    public function detach() {
        if ($this->_parent !== NULL) {
            $this->_parent->unsetColumn($this->getName());
        }
    }

    /**
     * accessor, parent column family name
     * @return string container name
     */
    public function getColumnFamilyName() {
        $parent = $this->getParent();
        if ($this->_columnFamilyName === NULL && $parent !== NULL) {
            return $parent->getName();
        }
        return $this->_columnFamilyName;
    }

    /**
     * mutator, container name
     * @param string $name new name
     */
    public function setColumnFamilyName($columnFamilyName) {
        $this->_columnFamilyName = $columnFamilyName;
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
     * Creates an error entry in this column and propogate to parent
     * @param string $errorStr error string
     */
    public function registerError($errorStr) {
        if (!empty($errorStr)) {
            array_push($this->errors, $errorStr);
            if ($this->_parent !== NULL) $this->_parent->registerError($errorStr);
        }
    }
}
?>