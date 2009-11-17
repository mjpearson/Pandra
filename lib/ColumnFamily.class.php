<?
/**
 * @todo everything! (stub only)
 */
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

	/**
	 * Constructor, builds column structures
	 */
	public function __construct($keyID = NULL) {		
		$this->constructColumns();
		if ($keyID !== NULL) $this->load($keyID);
	}	

	public function addColumn($colName, $validators = array(), $callbackOnSave = '') {
		if (!array_key_exists($colName, $this->columns)) {
			$this->columns[$colName] = array();
		}
		$this->columns[$colName]['validators'] = $validators;
		$this->columns[$colName]['callback'] = $callbackOnSave;
	}

	public function removeColumn($colName) {
		if (array_key_exists($colName, $this->columns)) {
			unset ($this->columns[$colName]);
		}
	}

	public function addSuper($superName, $columnsIn) {
		foreach ($columnsIn as $colName) {
			if (!array_key_exists($colName, $this->columns)) {
				// throw columnfamily exception ??
			}
			$this->columns[$colName]['super'] = $superName;
		}
	}


	/**
	 * Attempt to create a table abstraction on the fly (cache to filesystem).
	 * If class has been created already, its fine to use ptkcolumnfamily::Factory but prefereble to autload a db_$keySpace_$columnFamily object
	 * @param string $keySpace keyspace name
	 * @param string $columnFamily table name
	 * @return ptkcolumnfamily child class
	 */
	public static function Factory($keySpace, $columnFamily, $superColumn = NULL) {
	        if (!empty($superColumn)) $superColumn .= '_';
        
		$className = 'ptk_'.$keySpace.'_'.$superColumn.$columnFamily;
		if (!class_exists($className)) {			
			// create caching class
			if (class_exists('ptkcache')) {
				$ptkcache = new ptkcache();				
				$errors = "";
				
				// @todo check field signature
				if (!$ptkcache->factory($keySpace, $superColumn, $columnFamily, &$errors)) {
					throw new Exception($errors);
				}
			} else {
				throw new Exception('Factory could not build class.  ptkcache.class.php not found');
			}
		}		
		return new $className;
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
	 * Loads a row by it's keyID (all supercolumns and columns)
	 * @param mixed $value value of this rows primary key to load from
	 * @return bool this object has loaded its fields
	 */
	public function load($keyID, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
		if (empty($keyID)) return NULL;

	        $client = Pandra::getClient();

	        // build the column path
  		$columnParent = new cassandra_ColumnParent();
  		$columnParent->column_family = $this->columnFamily;
  		$columnParent->super_column = NULL;

  		$predicate = new cassandra_SlicePredicate();
  		$predicate->slice_range = new cassandra_SliceRange();
		$predicate->slice_range->start = '';
		$predicate->slice_range->finish = '';

  		$result = $client->get_slice($this->keySpace, $keyID, $columnParent, $predicate, $consistencyLevel);
	        // @todo load rcv data into local object
        	if (!empty($result)) {
			$this->keyID = $keyID;
			foreach ($result as $cObj) {
        	        	// populate self
				$c = $cObj->column;
				$this->columns[$c->name]['value'] = $c->value;
	            	}
        	    return TRUE;
	        }
        	return FALSE;
	}

	/**
	 * Save a record, based on this objects field values
	 * @return void
	 */
	public function save() {
		// check a keyID is defined
		if ($this->keyID === NULL) throw new Exception('NULL keyID defined, cannot insert');

		// check a Keyspace is defined
		if ($this->keySpace === NULL) throw new Exception('NULL keySpace defined, cannot insert');

		// check a column family is defined
		if ($this->columnFamily === NULL) throw new Exception('NULL columnFamliy defined, cannot insert');

	        $client = Pandra::getClient();

	        // @todo configurable consistency level
        	$consistency_level = cassandra_ConsistencyLevel::ZERO;

	        // build the column path
        	$columnPath = new cassandra_ColumnPath();
	        $columnPath->column_family = $this->columnFamily;

	        foreach ($this->columns as $colName => $cStruct) {
	        	$timestamp = time();
        	    	$columnPath->column = $colName;
//	        	$columnPath->super_column = null;

			// handle saving callback
			if (!empty($cStruct['callback'])) {
				eval('$cStruct[\'value\'] = '.$cStruct['callback'].'("'.$cStruct['value'].'");');
			}

// @todo supercolumn
//			if (!empty($cStruct['super'])) $columnPath->super_column = $cStruct['super'];
echo "insert ks:".$this->keySpace." id:".$this->keyID." cp:".$columnPath->column." sp:".$columnPath->super_column." v:".$cStruct['value']." ts:".$timestamp." cl:".$consistency_level."\n";
	            	$client->insert($this->keySpace, $this->keyID, $columnPath, $cStruct['value'], $timestamp, $consistency_level);
        	}
	}

	/**
	 * deletes the loaded object from the keyspace
	 */
	public function delete() {
		
		return FALSE;
	}


	/*
	 * Populates object with
	 * @param array key/value pair of field => value attributes
	 */
	protected function populate($data) {
		if (is_array($data)) {
			foreach ($data as $key => $value) {
				// store old validation flag
				//$oldValidate = $this->columns[$key]->validateEnable;
				//$this->columns[$key]->validateEnable = $validate;

				if (!array_key_exists($key, $this->columns)) throw new Exception("Class ".get_class($this)." appears out of date.  Missing field '".$key."'");

				// create prefixed magic set call
				$keyName = $this->_fieldPrefix.$key;
				$this->$keyName = $value;
				
				// restore validation flag
				//$this->columns[$key]->validateEnable = $oldValidate;
			}
		}
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
			if (!array_key_exists('value', $this->columns[$colName])) return NULL;
			return $this->columns[$colName]['value'];
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
			// @todo validation handler, exception?
			if (is_array($this->columns[$colName]['validators'])) {
				foreach ($this->columns[$colName]['validators'] as $validator) {
					list($validator, $args) = explode('=', $validators);
/*
	@todo: validator
					if (!Pandra::valid($validator, $args)) {
						return FALSE;
					}
*/
				}
			}
			$this->columns[$colName]['value'] = $value;
			return TRUE;
		}
		return FALSE;
	}

	/**
	 * Grab last error from field validator
	 * @return string validation error
	 */
	protected function getValidateError() {
		return $this->columns[$colName]->lastError();
	}

	/**
	 * gets the serialised k/v fields for this row
	 * @return string
	 */
	public function getFieldsSerialised() {
		// build temp k/v pair array for serialisation
		$columns = array();
		foreach ($this->columns as $colName => $ptkField) {
			$columns[$colName] = $ptkField->getValue();
		}
		return serialize($columns);
	}

	/**
	 * gets xml k/v fields
	 * @param string $header optional xml header (default v1.0/utf-8)
	 * @return string xml for this rows fields
	 */
	public function getFieldsXML($header = '<?xml version="1.0" encoding="utf-8"?>') {
		$xml = simplexml_load_string($header."<".str_replace(".", "_", $this->table)." />");
		foreach ($this->columns as $colName => $ptkField) {
			$xml->addChild($colName, $ptkField->getValue());
		}
		return $xml->asXML();
	}

	/**
	 * constructFields builds ptkField objects
	 */
	abstract public function constructColumns();
}
?>
