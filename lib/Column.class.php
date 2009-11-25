<?php
/**
 *
 * @package Pandra
 */
class PandraColumn {

	/* @var string column name */
	public $name = NULL;

	/* @var string column value */
	public $value = NULL;

	/* @var int last changed timestamp */
	public $timestamp = NULL;

	/* @var array validator type definitions for this colun */
	public $typeDef = array();

	/* @var string last processing error */
	public $lastError = NULL;

	/* @var string callback function for this column pre-save */
	public $callback = NULL;

	/* @var bool column value has been modified since load() or init */
	private $_modified = FALSE;

	/* @var $delete column is marked for deletion */
	private $_delete = FALSE;

	/* @var PandraColumnFamily column family parent reference */
	private $_parentCF = NULL;

	public function __construct($name, &$parentCF) {
		$this->name = $name;
		$this->_parentCF = $parentCF;
	}

	public function bindTime($timeOverride = NULL) {
		$this->timestamp = ($timeOverride === NULL) ? time() : $timeOverride;
		return $timestamp;
	}

        public function setValue($value, $validate = TRUE) {
            if ($validate && !empty($this->typeDef)) {
                if (!PandraValidator::check($value, $this->name, $this->typeDef, $this->lastError)) {
                    return FALSE;
                }
            }

            $this->value = $value;
            $this->_modified = TRUE;
            return TRUE;
        }

	public function reset() {
		$this->_modified = FALSE;
		$this->_delete = FALSE;
	}

	public function markDelete() {
		$this->_delete = TRUE;
		$this->_modified = TRUE;
	}

        public function callbackValue() {
            $value = '';
            eval('$value = '.$cObj->callback.'("'.$cObj->value.'");');
            return $value;
        }

	public function save() {

		if (!$this->_modified) return FALSE;

		// @todo configurable consistency
		$consistencyLevel = cassandra_ConsistencyLevel::ONE;

		$this->_parentCF->checkCFState();

		$client = Pandra::getClient(TRUE);

		// build the column path
       	        $columnPath = new cassandra_ColumnPath();
               	$columnPath->column_family = $this->_parentCF->columnFamily;
		$columnPath->column = $this->name;

		// @todo super writes
		if ($this->_parentSuper !== NULL) {
//			$columnPath->super_column = $this->_parentSuper->name;
		}

		if ($this->_delete) {
	                $client->remove($this->_parentCF->keySpace, $this->_parentCF->keyID, $columnPath, $this->bindTime(), $consistencyLevel);
		} else {
        	        $client->insert($this->_parentCF->keySpace, $this->_parentCF->keyID, $columnPath, $this->value, $this->bindTime(), $consistencyLevel);
		}

		return TRUE;
	}

	public function delete() {
		$this->markDelete();
		$this->save();
	}
}
