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

    const DEFAULT_ROW_LIMIT = 100;

    const THRIFT_PORT_DEFAULT = 9160;

    static public $lastError = '';

    static private $_consistencyLevel = cassandra_ConsistencyLevel::ONE;

    static private $_nodeConns = array();

    static private $_activeNode = NULL;

    static private $readMode = self::MODE_ACTIVE;

    static private $writeMode = self::MODE_ACTIVE;

    static private $_supportedModes = array(
            self::MODE_ACTIVE,
            self::MODE_ROUND,
            self::MODE_RANDOM,
    );

    static private $_memcachedAvailable = FALSE;

    static private $_apcAvailable = FALSE;

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
     * getTime stub will generate microsecond time (waiting on tbinaryprotocolaccellerated fix)
     * @return <type>
     */
    static public function getTime() {
        // use microtime where possible
        if (PHP_INT_SIZE == 8) {
            return round(microtime(true) * 1000, 3);
        }

        return time();
    }

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

    public function saveSuperColumn($keySpace, $keyID, $superCFName, $superColumnName, array $columns, $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);

            $scContainer = new cassandra_ColumnOrSuperColumn();
            $scContainer->super_column = new cassandra_SuperColumn();
            $scContainer->super_column->name = $superColumnName;
            $scContainer->super_column->columns = $columns;

            $mutation = array();
            $mutations[$superCFName] = array($scContainer);

            $client->batch_insert($keySpace, $keyID, $mutations, self::getConsistency($consistencyLevel));

        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Gets complete slice of Thrift cassandra_Column objects for keyID
     *
     * @return array cassandra_Column objects
     */
    public function getCFSlice($keyID, $keySpace, $columnFamilyName, $superColumnName = NULL, $consistencyLevel = NULL) {

        if (is_array($keyID)) return self::getRangeKeys($keyID, $keySpace, $columnFamilyName, $superColumnName, NULL, NULL, NULL, $consistencyLevel);

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

    public function getCFRange($keySpace, array $keyIDs, $columnFamilyName, $superColumnName = NULL, $columnNames = NULL, $rangeStart = NULL, $rangeFinish = NULL, $consistencyLevel = NULL) {

        $client = self::getClient();

        $columnParent = new cassandra_ColumnParent(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName
        ));
        $predicate = new cassandra_SlicePredicate(array(
                        'column_names' => $columnNames,
                        'slice_range' => new cassandra_SliceRange()
        ));

        if ($rangeStart !== NULL) $predicate->slice_range->start = $rangeStart;
        if ($rangeFinish !== NULL) $predicate->slice_range->finish = $rangeFinish;

        try {
            return $client->multiget_slice($keySpace, $keyIDs, $columnParent, $predicate, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }
    }

    /**
     * Returns by key, the column count in a column family (or a single CF/SuperColumn pair)
     * @param string $keyID row key id
     * @param string $keySpace keyspace of key
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
     * @param <type> $keySpace
     * @param <type> $columnFamilyName
     * @param <type> $columnName
     * @param <type> $keyStart
     * @param <type> $keyFinish
     * @param <type> $superColumnName
     * @param <type> $numRows
     * @param <type> $consistencyLevel
     * @return <type>
     */
    public function getRangeKeys($keySpace, $columnFamilyName, $columnNames, $superColumnName = NULL, $keyStart = '', $keyFinish = '', $numRows = self::DEFAULT_ROW_LIMIT, $consistencyLevel = NULL) {

        $client = self::getClient();

        $columnParent = new cassandra_ColumnParent(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName
        ));
        $predicate = new cassandra_SlicePredicate(array(
                        'column_names' => $columnNames,
                        'slice_range' => new cassandra_SliceRange()
        ));

        if ($rangeStart !== NULL) $predicate->slice_range->start = $rangeStart;
        if ($rangeFinish !== NULL) $predicate->slice_range->finish = $rangeFinish;

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