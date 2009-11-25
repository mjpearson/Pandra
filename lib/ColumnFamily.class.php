<?php
/**
 *
 * @package Pandra
 * @abstract
 */
abstract class PandraColumnFamily {

	/* @var string keyspace for this column family */
	public $keySpace = NULL;

	/* @var string child table name */
	public $columnFamily = NULL;

	/* @var string super column name for this columnfamily (supers may encapsulate columns and column families) */
	public $superColumn = NULL;

    	/* @var mixed keyID key for the working row */
    	public $keyID = NULL;

	/* @var string magic set/get prefixes */
	private  $_cFieldPrefix = 'column_';	// magic __get/__set column prefix in column famliy

	/* @var string magic set/get prefixes */
	private  $_sFieldPrefix = 'super_';	// magic __get/__set super prefix in column family

	/* @var array container for column objects, indexed to field name */
	protected $columns = array();

	/* @var array container for supers (column container objects), indexed to supercolumn name */
	protected $supers = array();

        /* @var string last error encountered */
        public $lastError = NULL;

        /* @var array complete list of errors for this object instance */
        public $errors = array();

	/* var bool columnfamily marked for deletion */
	private $_delete = FALSE;

	/**
	 * Constructor, builds column structures
	 */
	public function __construct($keyID = NULL) {		
		$this->constructColumns();
		if ($keyID !== NULL) $this->load($keyID);
	}	

	public function addColumn($colName, $typeDef = array(), $callbackOnSave = NULL) {
		if (!array_key_exists($colName, $this->columns)) {
			$this->columns[$colName] = new PandraColumn($colName, $this);
		}

                // array of validation functions
                if (!empty($typeDef)) $this->columns[$colName]->typeDef = $typeDef;

		// pre-save callback
		if (!empty($callbackOnSave)) $this->columns[$colName]->callback = $callbackOnSave;

                return $this->getColumn($colName);
	}

	public function getColumn($colName) {
		if (array_key_exists($colName, $this->columns)) {
			return $this->columns[$colName];
		}
		return NULL;
	}

	public function addSuper($superName) {
		if (!array_key_exists($superName, $this->supers)) {
			$this->supers[$superName] = new PandraSuperColumn($superName);
		}
		return $this->getSuper($superName);
	}

	public function getSuper($superName) {
		if (array_key_exists($superName, $this->supers)) {
			return $this->supers[$superName];
		}
		return NULL;
	}

	/**
	 * Gets complete slice of Thrift cassandra_Column objects for keyID
	 * 
	 * @return array cassandra_Column objects
	 */
	public function getRawSlice($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
		$this->keyID = $keyID;

		$this->checkCFState();

	        $client = Pandra::getClient();

	        // build the column path
  		$columnParent = new cassandra_ColumnParent();
  		$columnParent->column_family = $this->columnFamily;
  		$columnParent->super_column = $this->superColumn;

  		$predicate = new cassandra_SlicePredicate();
  		$predicate->slice_range = new cassandra_SliceRange();
		$predicate->slice_range->start = '';
		$predicate->slice_range->finish = '';

  		return $client->get_slice($this->keySpace, $keyID, $columnParent, $predicate, $consistencyLevel);
	}

	/**
	 * Loads a row by it's keyID (all supercolumns and columns)
	 * @todo super columns and slice loads
	 * @param mixed $value value of this rows primary key to load from
	 * @return bool this object has loaded its fields
	 */
	public function load($keyID, $colAutoCreate = FALSE, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
		$result = $this->getRawSlice($keyID, $consistencyLevel);
        	if (!empty($result)) {
			foreach ($result as $cObj) {
        	        	// populate self, skip validators - self trusted
				if ($colAutoCreate && !array_key_exists($cObj->column->name, $this->columns)) $this->addColumn($cObj->column->name);
				$this->columns[$cObj->column->name]->value = $cObj->column->value;
	            	}
        	    return TRUE;
	        }
        	return FALSE;
	}

	/**
	 * Check column family object state looks OK for writes
	 * @todo: make more dev friendly
	 */
	public function checkCFState() {
		// check a keyID is defined
		if ($this->keyID === NULL) throw new RuntimeException('NULL keyID defined, cannot insert');

		// check a Keyspace is defined
		if ($this->keySpace === NULL) throw new RuntimeException('NULL keySpace defined, cannot insert');

		// check a column family is defined
		if ($this->columnFamily === NULL) throw new RuntimeException('NULL columnFamliy defined, cannot insert');
	}

	/**
	 * Save all columns in this loaded columnfamily
	 * @return void
	 */
	public function save() {

		$this->checkCFState();

		if ($this->_delete === TRUE) {
		        $client = Pandra::getClient(TRUE);

			if ($columnPath === NULL) {
				$columnPath = new cassandra_ColumnPath();
			        $columnPath->column_family = $this->columnFamily;
			}

			$client->remove($this->keySpace, $this->keyID, $columnPath, time(), $consistencyLevel);
		}

	        foreach ($this->columns as &$cObj) {
			$cObj->save();
        	}
	}

	/**
	 * deletes the loaded object from the keyspace, or optionally the supplied columns for the key
	 */
	public function markDelete() {
		$this->_delete = TRUE;
		return;
	}

	public function unDelete() {
		$this->_delete = FALSE;
		return;
	}

	public function reset() {
		foreach  ($this->columns as &$cObj) {
			$cObj->reset();
		}
	}

	/*
	 * Populates object from $data array.  Bool false on validation error error, check $this->errors for messages
	 * @param array key/value pair of column => value
         * @return bool populated without validation errors
	 */
	protected function populate($data) {
            $errors = NULL;
            if (is_array($data)) {
                    foreach ($data as $key => $value) {
                            if (array_key_exists($key, $this->columns)) {
                                if (!$this->columns[$key]->setValue($value)) {
                                    $this->errors[] = $this->columns[$key]->lastError;
                                }
                            }
                    }
            }
            return empty($errors);
	}

	/**
	 * determine if get/set field exists/is mutable, strips field prefix from magic get/setters
	 * @param string $colName field name to check
	 * @return bool field exists
	 */
	private function gsMutable(&$colName) {
		$colName = preg_replace("/^".$this->_cFieldPrefix."/", "", strtolower($colName));
		return array_key_exists($colName, $this->columns);
	}

	/**
	 * Magic getter
	 * @param string $colName field name to get
	 * @return string value
	 */
	protected function __get($colName) {
		if ($this->gsMutable($colName)) {
			return $this->columns[$colName]->value;
		} else {
			return NULL;
		}
	}	

	/**
	 * Magic setter
	 * @param string $colName field name to set
	 * @param string $value  value to set for field
	 * @return bool field set ok
	 */
	protected function __set($colName, $value) {
		if ($this->gsMutable($colName)) {
			return $this->setColumn($colName, $value);
		}
		return FALSE;
	}

        /**
         * Sets a columns value for this slice
         * @param string $colName Column name to set
         * @param string $value New value for column
         * @param bool $validate opt validate from typeDef (default TRUE)
         * @return bool column set ok
         */
	public function setColumn($colName, $value, $validate = TRUE)  {
		if ($this->gsMutable($colName)) {
                    if ($this->columns[$colName]->setValue($value, $validate)) return TRUE;
                    $this->errors[] = $this->lastError = $this->columns[$colName]->lastError;
		}
		return FALSE;
	}
       
	/**
	 * constructFields builds column objects via addColumn/addSuper methods
	 */
	abstract public function constructColumns();
}
