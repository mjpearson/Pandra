<?
/**
 * Wrapper for a non-schema and non-validated slice (simple factory)
 * Loads all columns/supers in a columnfamily for given $keyID
 * @package Pandra
 */
class PandraSlice extends PandraColumnFamily {

	

	public function constructColumns() { }

	public function __construct($keySpace, $columnFamily, $keyID = NULL) {
		$this->keySpace = $keySpace;
		$this->columnFamily = $columnFamily;

		if ($keyID !== NULL) {
			$this->load($keyID);
		}
	}

	public function setSliceColumns($columns = array()) {
	}

	public function load($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
		parent::load($keyID, TRUE, $consistencyLevel);
	}
}
