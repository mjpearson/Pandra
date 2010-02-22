<?php

/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @link http://www.phpgrease.net/projects/pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */
class PandraCore {


    const MODE_ACTIVE = 0; // Active client only

    const MODE_ROUND = 1; // sequentially select configured clients

    const MODE_RANDOM = 2; // select random node

    const DEFAULT_ROW_LIMIT = 100; // default max # of rows to return for ranging queries

    const THRIFT_PORT_DEFAULT = 9160; // Default Thrift port

    /* @var string Last internal error */
    static public $lastError = '';

    /* @var int default consistency level */
    static private $_consistencyLevel = cassandra_ConsistencyLevel::ONE;

    /* @var array available transports */
    static private $_nodeConns = array();

    /* @var string currently selected node */
    static private $_activeNode = NULL;

    /* @var int default read mode (active/round/random) */
    static private $readMode = self::MODE_RANDOM;

    /* @var int default write mode (active/round/random) */
    static private $writeMode = self::MODE_RANDOM;

    /* @var supported modes for this core version */
    static private $_supportedModes = array(
            self::MODE_ACTIVE,
            self::MODE_ROUND,
            self::MODE_RANDOM,
    );

    /* @var bool Memcached is available for use */
    static private $_memcachedAvailable = FALSE;

    /* @var bool APC is available for use */
    static private $_apcAvailable = FALSE;

    /**
     * Supported Modes accessor
     * @return array supported read/write modes
     */
    static public function getSupportedModes() {
        return self::$_supportedModes;
    }

    static public function setReadMode($newMode) {
        if (!array_key_exists($newMode, self::$_supportedModes)) throw new RuntimeExcpetion("Unsupported Read Mode");
        self::$readMode = $newMode;
    }

    static public function getReadMode() {
        return self::$readMode;
    }

    static public function setWriteMode($newMode) {
        if (!array_key_exists($newMode, self::$_supportedModes)) throw new RuntimeExcpetion("Unsupported Write Mode");
        self::$writeMode = $newMode;
    }

    static public function getWriteMode() {
        return self::$writeMode;
    }


    /**
     *
     */
    static public function setActiveNode($connectionID) {
        if (array_key_exists($connectionID, self::$_nodeConns) && self::$_nodeConns[$connectionID]['transport']->isOpen()) {
            self::$_activeNode = $connectionID;
            return TRUE;
        }
        return FALSE;
    }

    /**
     *
     */
    static public function disconnect($connectionID) {
        if (array_key_exists($connectionID, self::$_nodeConns)) {
            if (self::$_nodeConns[$connectionID]['transport']->isOpen()) {
                self::$_nodeConns[$connectionID]['transport']->close();
                return TRUE;
            }
        }
        return FALSE;
    }

    /**
     *
     */
    static public function disconnectAll() {

        $connections = array_keys(self::$_nodeConns);

        foreach ($connections as $connectionID) {
            if (!self::disconnect($connectionID)) throw new RuntimeException($connectionID.' could not be closed');
        }
        return TRUE;
    }

    /**
     * Connects to given Cassandra node and makes it available in the static connection pool
     * @param string $connectionID named node
     * @param string $host host name or IP of connecting node
     * @param int $port TCP port of connecting node
     * @return bool connected ok
     */
    static public function connect($connectionID, $host = NULL, $poolName = 'default', $port = self::THRIFT_PORT_DEFAULT) {
        try {
            // if the connection exists but it is closed, then re-open
            if (array_key_exists($connectionID, self::$_nodeConns)) {
                if (!self::$_nodeConns[$connectionID]['transport']->isOpen()) {
                    self::$_nodeConns[$connectionID]['transport']->open();
                }
                return TRUE;
            }

            // Create Thrift transport and binary protocol cassandra client
            $transport = new TBufferedTransport(new TSocket($host, $port), 1024, 1024);
            $transport->open();

            self::$_nodeConns[$connectionID] = array(
                    'transport' => $transport,
                    'client' => new CassandraClient((function_exists("thrift_protocol_write_binary") ? new TBinaryProtocolAccelerated($transport) : new TBinaryProtocol($transport)))
            );

            // set new connection the active, working master
            self::setActiveNode($connectionID);
            return TRUE;
        } catch (TException $tx) {
            self::$lastError = 'TException: '.$tx->getMessage() . "\n";
        }
        return FALSE;
    }

    static public function setMemcachedAvailable($memcachedAvailable) {
        self::$_memcachedAvailable = $memcachedAvailable;
    }

    static public function getMemcachedAvailable() {
        return self::$_memcachedAvailable;
    }

    static public function setAPCAvailable($apcAvailable) {
        self::$_apcAvailable = $apcAvailable;
    }

    static public function getAPCAvailable() {
        return self::$_apcAvailable;
    }

    /**
     * get current working node, recursive, trims disconnected clients
     */
    static public function getClient($writeMode = FALSE) {
        if (empty(self::$_activeNode)) throw new Exception('Not Connected');
        $useMode = ($writeMode) ? self::$writeMode : self::$readMode;
        switch ($useMode) {
            case self::MODE_ROUND :
                if (!current(self::$_nodeConns)) reset(self::$_nodeConns);
                $curConn = each(self::$_nodeConns);
                self::$_activeNode = $curConn['key'];		// store current working node
                return self::$_nodeConns[self::$_activeNode]['client'];
                break;
            case self::MODE_RANDOM :
                $randConn =& array_rand(self::$_nodeConns);
                return self::$_nodeConns[$randConn]['client'];
                break;
            case self::MODE_ACTIVE :
            default :
                return self::$_nodeConns[self::$_activeNode]['client'];
                break;
        }
    }

    static public function describeKeyspace($keySpace) {
        $client = self::getClient();
        return $client->describe_keyspace($keySpace);
    }

    static public function getConsistency($override = NULL) {
        $consistency = self::$_consistencyLevel;
        if ($override !== NULL) {
            $consistency = $override;
        }

        return $consistency;
    }

    static public function setConsistency(int $consistencyLevel) {
        self::$_consistencyLevel = $consistencyLevel;
    }

    static public function loadConfigXML() {
        if (!file_exists(CASSANDRA_CONF_PATH)) {
            throw new RuntimeException('Cannot build models, file not found ('.CASSANDRA_CONF_PATH.')\n');
        }

        $conf = simplexml_load_file(CASSANDRA_CONF_PATH);
        return $conf;
    }

    /**
     * Generates current time, or microtime for 64-bit systems
     * @return int timestamp
     */
    static public function getTime() {
        // use microtime where possible
        if (PHP_INT_SIZE == 8) {
            return round(microtime(true) * 1000, 3);
        }
        return time();
    }

    /**
     * Deletes a Column Path from Cassandra
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnPath $columnPath
     * @param int $time deletion timestamp
     * @param int $consistencyLevel response consistency level
     * @return bool Column Path deleted OK
     */
    public function deleteColumnPath($keySpace, $keyID, cassandra_ColumnPath $columnPath, $time = NULL, $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);
            if ($time === NULL) {
                $time = self::getTime();
            }
            $client->remove($keySpace, $keyID, $columnPath, $time, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Saves a Column Family to Cassandra
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnPath $columnPath
     * @param int $time deletion timestamp
     * @param int $consistencyLevel response consistency level
     * @return bool Column Path saved OK
     */
    public function saveColumnPath($keySpace, $keyID, cassandra_ColumnPath $columnPath, $value,  $time = NULL, $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);
            if ($time === NULL) {
                $time = self::getTime();
            }

            $client->insert($keySpace, $keyID, $columnPath, $value, $time, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Batch saves a SuperColumn and its modified Columns to Cassandra
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param string $superCFName Super Column Family name
     * @param string $superColumnName array of Super Column Family name (string) keyed to array of modified cassandra_Columns
     * @param int $consistencyLevel response consistency level
     * @return bool Super Column saved OK
     */
    public function saveSuperColumn($keySpace, $keyID, $superCFName, array $superColumnMap, $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);

            $mutation = array();
            foreach ($superColumnMap as $superColumnName => $columns) {
                $scContainer = new cassandra_ColumnOrSuperColumn();
                $scContainer->super_column = new cassandra_SuperColumn();
                $scContainer->super_column->name = $superColumnName;
                $scContainer->super_column->columns = $columns;

                $mutations[$superCFName] = array($scContainer);
            }

            $client->batch_insert($keySpace, $keyID, $mutations, self::getConsistency($consistencyLevel));

        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Gets complete slice of Thrift cassandra_Column objects for keyID
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param <type> $columnFamilyName
     * @param <type> $superColumnName
     * @param <type> $consistencyLevel
     * @return cassandra_Column Thrift cassandra column
     */
    public function getCFSlice($keySpace, $keyID, $columnFamilyName, $superColumnName = NULL, $consistencyLevel = NULL) {

        if (is_array($keyID)) return self::getCFSliceMulti($keySpace, $keyID, $columnFamilyName, $superColumnName, NULL, NULL, NULL, $consistencyLevel);

        $client = self::getClient();

        // build the column path
        $columnParent = new cassandra_ColumnParent();
        $columnParent->column_family = $columnFamilyName;
        $columnParent->super_column = $superColumnName;

        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = new cassandra_SliceRange();
        $predicate->slice_range->start = '';
        $predicate->slice_range->finish = '';

        try {
            return $client->get_slice($keySpace, $keyID, $columnParent, $predicate, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }
        return NULL;
    }

    /**
     * Retrieves slice of columns across keys in parallel
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param string $columnFamilyName column family name
     * @param string $superColumnName optional super column name
     * @param array $columnNames optional array of column names
     * @param string $rangeStart optional column range start
     * @param string $rangeFinish optional column range end
     * @param int $consistencyLevel response consistency level
     * @return <type>
     */
    public function getCFSliceMulti($keySpace, array $keyIDs, $columnFamilyName, $superColumnName = NULL, $columnNames = NULL, $consistencyLevel = NULL) {

        $client = self::getClient();

        $columnParent = new cassandra_ColumnParent(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName
        ));
        $predicate = new cassandra_SlicePredicate(array(
                        'column_names' => $columnNames,
                        'slice_range' => new cassandra_SliceRange()
        ));

        $predicate->slice_range->start = '';
        $predicate->slice_range->finish = '';

        try {
            return $client->multiget_slice($keySpace, $keyIDs, $columnParent, $predicate, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }
    }

    /**
     * Returns by key, the column count in a column family (or a single CF/SuperColumn pair)
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param string $columnFamilyName column family name
     * @param string $superColumnName optional super column name
     * @param int $consistencyLevel response consistency level
     * @return int number of rows or null on error
     */
    public function getCFColumnCount($keySpace, $keyID, $columnFamilyName, $superColumnName = NULL, $consistencyLevel = NULL) {
        $client = self::getClient();

        $columnParent = new cassandra_ColumnParent(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName
        ));
        try {
            return $client->get_count($keySpace, $keyID, $columnParent, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }
    }

    /**
     * Retrieves a single column of a column family for key
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param string $columnFamilyName column family name
     * @param string $columnName column name
     * @param string $superColumnName optional super column name
     * @param int $consistencyLevel
     * @return cassandra_Column Thrift cassandra column
     */
    public function getColumn($keySpace, $keyID, $columnFamilyName, $columnName, $superColumnName = NULL, $consistencyLevel = NULL) {

        $client = self::getClient();

        $columnPath = new cassandra_ColumnPath(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName,
                        'column' => $columnName
        ));

        try {
            return $client->get($keySpace, $keyID, $columnPath, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }
    }

    /**
     *
     * @param string $keySpace keyspace of key
     * @param string $columnFamilyName column family name
     * @param array $columnName array of column names
     * @param int optional number of rows to return
     * @param array $rangeOptions optional keyed array of cassandra_SliceRange options
     * @param int $consistencyLevel
     * @return <type>
     */
    public function getRangeKeys($keySpace, $columnFamilyName, $columnNames, $superColumnName = NULL, $numRows = self::DEFAULT_ROW_LIMIT, $rangeOptions = array(), $consistencyLevel = NULL) {

        $client = self::getClient();

        $columnParent = new cassandra_ColumnParent(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName
        ));

        $predicate = new cassandra_SlicePredicate(array(
                        'column_names' => $columnNames,
                        'slice_range' => new cassandra_SliceRange()
        ));

        if (empty($rangeOptions)) {
            $rangeOptions = array('start' => '', 'finish' => '');
        }

        foreach ($rangeOptions as $option => $value) {
            $predicate->slice_range->$option = $value;
        }

        try {
            return $client->get_range_slice($keySpace, $columnParent, $predicate, $keyStart, $keyFinish, $numRows, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }

    }


    /**
     * Grabs locally defined columnfamilies (debug tool)
     */
    /*
    static public function getConfColumnFamilies() {

        $conf = self::loadConfigXML();

        $columnFamiles = array();

        foreach ($conf->Keyspaces as $keySpace) {
            $ksName = $keySpace->attributes()->Name;
            $columnFamilies[] = $keySpace->xpath('Keyspace/ColumnFamily');
        }

        return $columnFamiles;
    }

    static public function buildModels() {
        // check the schemas directory
        if (file_exists(SCHEMA_PATH)) {

            // Grab config, check our available keyspaces
            $dir = scandir(SCHEMA_PATH);
            foreach ($dir as $fileName) {
                if (preg_match('/^\./', $fileName)) continue;
                $filePath = SCHEMA_PATH.'/'.$fileName;
                $schema = NULL;

                list($keySpace, $extension) = explode('.', $fileName);

                $extension = strtolower($extension);
                if ($extension == 'json') {
                    $c = file_get_contents($filePath);
                    $schema = json_decode(file_get_contents($filePath));
                } else if ($extension == 'yaml') {
                    if (!function_exists('syck_load')) {
                        throw new RuntimeException('YAML schema found but syck module not supported');
                    } else {
                        $schema = syck_load($filePath);
                    }
                }

                if ($schema === NULL) {
                    throw new RuntimeException('Schema failed to parse ('.$filePath.')');
                } else {

                }
            }
        } else {
            throw new RuntimeException('Defined SCHEMA_PATH not found ('.SCHEMA_PATH.')');
        }

        // Check if syck module is available
        //if (!function_exists('syck_load')) {
        //}
    }
    */
}

// Setup our capabilities
PandraCore::setMemcachedAvailable(class_exists('Memcached'));

PandraCore::setAPCAvailable(function_exists('apc_sma_info') && apc_sma_info() !== FALSE);
?>