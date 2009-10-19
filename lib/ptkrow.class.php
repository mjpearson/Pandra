<?
/**
 * @todo everything! (stub only)
 */
abstract class ptkrow {

	/* @var string magic set/get prefixes */
	const _fieldPrefix = 'field';	// magic __get/__set column prefix

	/* @var bool flag indicates whether object was 'loaded' */
	private $_loaded = FALSE;

	/* @var array container for ptkField objects, indexed to field name */
	protected $columns = array();

	/* @var string keyspace for this column family */
	public $keySpace = NULL;

    /* @var string supercolumn name */
    public $superColumn = NULL;

	/* @var string child table name */
	public $columnFamily = NULL;

    /* @var mixed keyID key for the working row */
    public $keyID = NULL;

	/**
	 * Constructor, builds ptkFields structures
	 */
	public function __construct() {		
		$this->constructColumns();
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
	 * Loads a row by it's keyid (all supercolumns and columns)
	 * @param mixed $value value of this rows primary key to load from
	 * @return bool this object has loaded its fields
	 */
	public function load($keyId, $consistencyLevel = cassandra_ConsistencyLevel::ZERO) {
		if (empty($keyId)) return NULL;

        $client = ptk::getClient();

        // build the column path
        $columnPath = new cassandra_ColumnPath();
        $columnPath->column_family = $this->columnFamily;
        $columnPath->super_column = NULL;

        $rcv = $client->get($this->keySpace, $keyId, $columnPath, $consistencyLevel);

        // @todo load rcv data into local object
        if (!empty($rcv)) {
            while ($column = each($rcv)) {
                // populate self
            }
            return TRUE;
        }
        return FALSE;
	}

	/**
	 * Insert a row, based on this objects field values
	 * @return void
	 */
	public function insert() {

        $client = ptk::getClient();
        $timestamp = time();

        // @todo configurable consistency level
        $consistency_level = cassandra_ConsistencyLevel::ZERO;

        // build the column path
        $columnPath = new cassandra_ColumnPath();
        $columnPath->column_family = $this->columnFamily;
        $columnPath->super_column = $this->superColumn;

        foreach ($this->columns as $columnName => $value) {
            $columnPath->column = $columnName;
            $client->insert($this->keySpace, $this->keyID, $columnPath, $value, $timestamp, $consistency_level);
        }
	}

	/**
	 * Update a row in place, based on loaded keys
	 * @return void
	 */
	public function update() {
	}	

	/**
	 * Grab primary key field values
	 * @param string $clause field name clause
	 * @param array $vals reference to values
	 * @return void
	 */
	private function pKeyFieldVals(&$clause, &$vals) {		
		foreach ($this->_pKeys as $columnName) {
			$value = $this->columns[$columnName]->getValue();
			if (empty($value)) {				
				// @todo: throw an exception here?
				return FALSE;
			} else {
				if ($clause != '') $clause .= ',';
				$clause .= trim(ptk::quote($columnName), "'")." = ".ptk::quote($value);
				$vals[] = $value;
			}
		}
		return TRUE;
	}

	/**
	 * deletes the loaded object from the keyspace
	 */
	public function delete() {
		
		if (!$this->_loaded) return FALSE;

		// check pkeys are populated
		$whereClause = '';
		$whereVals = array();
		
		if (!$this->pKeyFieldVals($whereClause, $whereVals)) return FALSE;

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
				$keyName = self::_fieldPrefix.$key;
				$this->$keyName = $value;
				
				// restore validation flag
				//$this->columns[$key]->validateEnable = $oldValidate;
			}
		}
	}

	/**
	 * determine if get/set field exists/is mutable, strips field prefix from magic get/setters
	 * @param string $columnName field name to check
	 * @return bool field exists
	 */
	private function gsMutable(&$columnName) {
		$columnName = preg_replace("/^".self::_fieldPrefix."/", "", strtolower($columnName));
		return array_key_exists($columnName, $this->columns);
	}

	/**
	 * Magic getter
	 * @param string $columnName field name to get
	 * @return string value
	 */
	protected function __get($columnName) {
		if ($this->gsMutable($columnName)) {
			return $this->columns[$columnName]->getValue();
		} else {
			return NULL;
		}
	}	

	/**
	 * Magic setter
	 * @param string $columnName field name to set
	 * @param string $value  value to set for field
	 * @return bool field set ok
	 */
	protected function __set($columnName, $value) {
		if ($this->gsMutable($columnName)) {
			// @todo validation handler, exception?
			$this->columns[$columnName]->setValue($value);
			return TRUE;
		}
		return FALSE;
	}

	/**
	 * Grab last error from field validator
	 * @return string validation error
	 */
	protected function getValidateError() {
		return $this->columns[$columnName]->lastError();
	}

	/**
	 * gets the serialised k/v fields for this row
	 * @return string
	 */
	public function getFieldsSerialised() {
		// build temp k/v pair array for serialisation
		$columns = array();
		foreach ($this->columns as $columnName => $ptkField) {
			$columns[$columnName] = $ptkField->getValue();
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
		foreach ($this->columns as $columnName => $ptkField) {
			$xml->addChild($columnName, $ptkField->getValue());
		}
		return $xml->asXML();
	}

	/**
	 * constructFields builds ptkField objects
	 */
	abstract public function constructFields();
}
?>
