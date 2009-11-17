<?
/**
 * **Experimental** class ColumnFamily builder
 *
 * Without an authoritive key id and with a keyspace holding any arbitrary data structure,
 * PandraFactory will make a guess attempt for the required schema based on the column
 * names of the first row.
 *
 * Guess attempts can only be run against Cassandra instances running an Order Preserving Partitioner
 *
 * @todo fix errors system
 * @todo complete class template writer
 */
class PandraFactory {

	private $fieldList = '';
	private $keySpace = '';
	private $columnFamily = '';
    private $superColumn = '';

	private $factoryMD5 = '';
	
    const cacheDIR = "/cache";

	/**
	 * factory method tries to create the columnFamily class in-situ, no merge or options
	 * @param string $keySpace keySpace name
	 * @param string $columnFamily column family name
	 * @param string $errors errors reference variable
	 * @return bool columnFamily created
	 */
	public function factory($keySpace, $superColumn, $columnFamily, &$errors, $keyAuthoritive = NULL) {
		if (!$this->factorColumnFamily($keySpace, $superColumn, $columnFamily)) {
            if (!empty($superColumn)) $superColumn = '`'.$superColumn.'`.';
			$errors = 'ERROR: Factor Failed, no columns in `'.$keySpace.'`.$superColumn.`'.$columnFamily.'`';
            return FALSE;
		}
        return TRUE;
	}    

    /**
     * Factory wrapper for authoritive keys
     * @return bool cache object created
     */
    public function factoryKeyed($keyID, $keySpace, $superColumn, $columnFamily, &$errors) {
        return $this->factory($keySpace, $superColumn, $columnFamily, $errors);
    }

    /**
	 * Turns a schema into a named list of ptkField constructors
     * @return bool columnFamily was found and factor-ised from the template
     */
    public function factorColumnFamily($keySpace, $superColumn, $columnFamily) {
		$this->fieldList = "";
		$this->keySpace = $keySpace;
		$this->columnFamily = $columnFamily;
        $this->superColumn = $superColumn;

        $client = ptk::getClient();

        $consistencyLevel = cassandra_ConsistencyLevel::QUORUM;

        $columnParent = new cassandra_ColumnParent();
        $columnParent->column_family = $columnFamily;
        $columnParent->super_column = $superColumn;

        $sliceRange = new cassandra_SliceRange();
        $sliceRange->start = "";
        $sliceRange->finish = "";

        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = $sliceRange;

        $keys = $client->get_key_range($keySpace, $columnFamily, 0, 1000, 1, cassandra_ConsistencyLevel::ZERO);

        if (empty($keys)) {
            throw new Exception("empty Key Space ($keySpace/$columnFamily)");
        }

        $row = $client->get_slice($keySpace, array_pop($keys), $columnParent, $predicate, cassandra_ConsistencyLevel::QUORUM);

        //
        return;

		// build field list
		if (!empty($schemaReal)) {
/*
			foreach ($schemaReal[$keySpace.".".$columnFamily] as $data) {
				switch ($data['Key']) {
					case 'PRI':
							$keyType = ", ptkField::TYPE_PKEY";
							break;
					default :
							$keyType = '';
				}

				$autoIncrement = '';
				if (!empty($data['Extra']) && $data['Extra']) {
					$autoIncrement = ", TRUE";
				}

				if ( empty($data['Default']) || $data['Default'] == '0000-00-00 00:00:00' || $data['Default'] == '0000-00-00') {
					$default = 'NULL';
				} else {
					$default = "'".$data['Default']."'";
				}
				// @todo, validation callback, crypt callback
				$this->fieldList .= "		\$this->fields['".$data['Field']."'] = new ptkField(".$default.",''".$keyType.$autoIncrement.");\n";
			}			

			$this->factoryMD5 = md5(serialize($schemaReal));
 * 
 */
			return TRUE;
		}
		return FALSE;
	}

	/**
	 * Writes templated class file
	 * @param bool $merge attempt to merge against existing factory file, where non-template methods have been defined
	 */
	public function write($merge = FALSE) {
		
		// open template
		$template = file_get_contents(dirname(__FILE__).'/ptkColumnFamilyRow.tpl');

		$template = preg_replace("/__DB__/", $this->keySpace, $template);
		$template = preg_replace("/__TABLE__/", $this->columnFamily, $template);
		$template = preg_replace("/__FIELDS__/", $this->fieldList, $template);
		$template = preg_replace("/__MD5__/", $this->factoryMD5, $template);
		
		$wDir = ptk::getDir().'/cache/'.strtolower($this->keySpace).'/';
        $wFileName = $wDir.strtolower('ptk_'.$this->keySpace.'_'.$this->columnFamily.'.class.php');

		// create keySpace directory if it doesn't exist
		if (!is_dir($wDir)) {			
			mkdir($wDir);
		}
		
		// write columnFamily cache file if we can
		if (!file_exists($wFileName)) {
			file_put_contents($wFileName, $template);
			return TRUE;
		} else {
			if ($merge) {
				// @todo: merge
			} else {
				throw new Exception("'".$wFileName."' exists, select 'Merge' to overwrite");
			}
		}
	}	

	/**
	 * Reports whether all cached schemas match current.
     * (Cache will not take any action where the schemas differ)
     * @param array $messages container for schemaCheck message srings
	 * @param mixed $columnFamilys string or array of columnFamilys to check.  Leave empty to check entire cache
	 * @return bool all signature tests passed
	 */
	public function schemaCheck(&$messages, $columnFamilys = array()) {
        if (!is_array($columnFamilys)) $columnFamilys = array($columnFamilys);
        $errors = FALSE;
        
        if (empty($columnFamilys)) {            
			// search entire cache

			$baseDir = ptk::getDir()."/".self::cacheDIR;

			$dh = opendir($baseDir);
			if (!$dh) {
				$messages[] = 'ERROR:'.ptk::getDir()."/".self::cacheDIR.':Could not open directory for processing';
				return FALSE;
			} else {
				 while (false !== ($dbDir = readdir($dh))) {
					// drop into keySpace directories
					$wBaseDir = $baseDir."/".$dbDir;
					if (is_dir($wBaseDir) && $dbDir != "." && $dbDir != "..") {
						$dht = opendir($wBaseDir);
						if (!$dht) {
							$messages[] = 'ERROR:$wBaseDir:Could not open directory for processing';
							return FALSE;
						} else {
							// check each defined cache file against the current connection
							while (false !== ($cacheFile = readdir($dht))) {
								if (strpos($cacheFile, "ptk_".$dbDir) !== FALSE) {
									$className = preg_replace("/\.class\.php$/", "", $cacheFile);

									$checkClass = new $className;
									list($db, $tbl) = explode(".", $checkClass->columnFamily);
									if (!$this->factorColumnFamily($db, $tbl)) {
										$messages[] = "ERROR:".$checkClass->columnFamily.":Class exists but the columnFamily could not be found via the active keySpace connection.";
										$errors = TRUE;
									} else {
										if ($this->factoryMD5 != $checkClass->factoryMD5) {
											$messages[] = "ERROR:".$checkClass->columnFamily.":MD5 mismatch, Cache ".$checkClass->factoryMD5.",DB ".$this->factoryMD5;
											$errors = TRUE;
										} else {
										   $messages[] = "OK:".$checkClass->columnFamily;
										}
									}
								}
							}
							closedir($dht);
						}
					}
				 }
				closedir($dh);
			}
        } else {
            foreach ($columnFamilys as $columnFamily) {
                list($db, $tbl) = explode(".", $columnFamily);
                if (empty($db) || empty($tbl)) {
					$messages[] = "ERROR:$columnFamily:Expected '\${keySpace name}.\${columnFamily name}'";
					$errors = TRUE;
					continue;
                }
                $className = "ptk_".$db."_".$tbl;
                if (class_exists($className)) {
					$checkClass = new $className;
                    if (!$this->factorColumnFamily($db, $tbl)) {
						$messages[] = "ERROR:$columnFamily:ColumnFamily not found";
						$errors = TRUE;
					} else {
						if ($this->factoryMD5 != $checkClass->factoryMD5) {
							$messages[] = "ERROR:$columnFamily:MD5 mismatch, Cache ".$checkClass->factoryMD5.",DB ".$this->factoryMD5;
							$errors = TRUE;
						} else {
						   $messages[] = "OK:$columnFamily";
						}
					}
                } else {
                    $messages[] = "ERROR:$columnFamily:Class ".$className." not found";
                    $errors = TRUE;
                }
            }
        }
        return !$errors;
	}
}
?>
