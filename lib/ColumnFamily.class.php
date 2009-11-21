<?
abstract class PandraColumnFamily {

	/* @var string magic set/get prefixes */
	private  $_fieldPrefix = 'col';	// magic __get/__set column prefix

	/* @var bool flag indicates whether object was 'loaded' */
	private $_loaded = FALSE;

	/* @var array container for ptkField objects, indexed to field name */
	protected $columns = array();

	/* @var string keyspace for this column family */
	public $keySpace = NULL;

	/* @var string child table name */
	public $columnFamily = NULL;

    	/* @var mixed keyID key for the working row */
    	public $keyID = NULL;

        /* */
        public $lastError = NULL;

        /* */
        public $errors = NULL;

	/**
	 * Constructor, builds column structures
	 */
	public function __construct($keyID = NULL) {		
		$this->constructColumns();
		if ($keyID !== NULL) $this->load($keyID);
	}	

	public function addColumn($colName, $typeDef = array(), $callbackOnSave = NULL) {
		if (!array_key_exists($colName, $this->columns)) {
			$this->columns[$colName] = new PandraColumn();
                        $this->columns[$colName]->name = $colName;
		}

                // array of validation functions
                if (!empty($typeDef)) $this->columns[$colName]->typeDef = $typeDef;

		// function callback pre-save
		if (!empty($callbackOnSave)) $this->columns[$colName]->callback = $callbackOnSave;

                return TRUE;
	}

	public function removeColumn($colName) {
		if (array_key_exists($colName, $this->columns)) {
			unset ($this->columns[$colName]);
		}
	}

	public function addSuper($superName, $columnsIn = array()) {
		if (!empty($columnsIn)) {
			foreach ($columnsIn as $colName) {
				if (!array_key_exists($colName, $this->columns)) {
					// throw columnfamily exception ??
				}
				$this->columns[$colName]['super'] = $superName;
			}
		}
	}

	/**
	 * returns array of matching rows by $search on this table
	 * @param array $search
	 * @param int $limit
	 * @return array
	 */
	static protected function findByKey($value) {
        	// @todo create a clone of this object, populate it with load data and return
	}

	/**
	 * Gets complete slice of Thrift cassandra_Column objects for keyID
	 * 
	 * @return array cassandra_Column objects
	 */
	public function getRawSlice($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
		$this->keyID = $keyID;

		$client = $this->_getClient();

	        // build the column path
  		$columnParent = new cassandra_ColumnParent();
  		$columnParent->column_family = $this->columnFamily;
  		$columnParent->super_column = NULL;

  		$predicate = new cassandra_SlicePredicate();
  		$predicate->slice_range = new cassandra_SliceRange();
		$predicate->slice_range->start = '';
		$predicate->slice_range->finish = '';

  		return $client->get_slice($this->keySpace, $keyID, $columnParent, $predicate, $consistencyLevel);
	}

	/**
	 * Loads a row by it's keyID (all supercolumns and columns)
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

	private function _getClient() {
		// check a keyID is defined
		if ($this->keyID === NULL) throw new RuntimeException('NULL keyID defined, cannot insert');

		// check a Keyspace is defined
		if ($this->keySpace === NULL) throw new RuntimeException('NULL keySpace defined, cannot insert');

		// check a column family is defined
		if ($this->columnFamily === NULL) throw new RuntimeException('NULL columnFamliy defined, cannot insert');

	        return Pandra::getClient();
	}

	/**
	 * Save a record, based on this objects field values
	 * @return void
	 */
	public function save() {

		$client = $this->_getClient();

	        // @todo configurable consistency level
        	$consistencyLevel = cassandra_ConsistencyLevel::ONE;

	        // build the column path
        	$columnPath = new cassandra_ColumnPath();
	        $columnPath->column_family = $this->columnFamily;

	        foreach ($this->columns as $colName => $cObj) {
			if (!$cStruct['modified']) continue;

	        	$timestamp = time();
        	    	$columnPath->column = $colName;
	        	$columnPath->super_column = null;

			// handle saving callback
                        $value = $cObj->value;
			if (!empty($cObj->callback)) {
                                $value = $cObj->callbackValue();
			}
                        
                        // @todo supercolumn container
	            	$client->insert($this->keySpace, $this->keyID, $columnPath, $cStruct['value'], $timestamp, $consistencyLevel);
        	}
	}

	/**
	 * deletes the loaded object from the keyspace, or optionally the supplied columns for the key
	 */
	public function delete(cassandra_ColumnPath $columnPath = NULL, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {

		$client = $this->_getClient();

		if ($columnPath === NULL) {
			$columnPath = new cassandra_ColumnPath();
		        $columnPath->column_family = $this->columnFamily;
		}

		$client->remove($this->keySpace, $this->keyID, $columnPath, time(), $consistencyLevel);

		return FALSE;
	}

	public function deleteColumn($column) {
		throw new RuntimeException('Not Implemented');

		// build column path
		$columnPath = new cassandra_ColumnPath();
	        $columnPath->column_family = $this->columnFamily;
	        $columnPath->column = $column;
	}

	public function deleteSuperColumn() {
		// delete columns in super column
		throw new RuntimeException('Not Implemented');
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
                                    $this->errors .= $this->columns[$key]->lastError;
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
		$colName = preg_replace("/^".$this->_fieldPrefix."/", "", strtolower($colName));
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
                    if (!$this->columns[$colName]->setValue($value, $validate)) {
                        $this->errors .= $this->lastError = $this->columns[$colName]->lastError;
                        return FALSE;
                    }
                    return TRUE;
		}
		return FALSE;
	}
       
	/**
	 * constructFields builds ptkField objects
	 */
	abstract public function constructColumns();
}
?>
