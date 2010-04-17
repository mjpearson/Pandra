<?php
/**
 * PandraCore
 *
 * Core is a gracefully degrading static connection manager, Thrift API helper and
 * cache manager
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.2
 * @package pandra
 */
class PandraCore {

    const MODE_ACTIVE = 0; // Active client only

    const MODE_ROUND = 1; // sequentially select configured clients

    const MODE_RANDOM = 2; // select random node

    const DEFAULT_POOL_NAME = 'default';

    /* @var string Last internal error */
    static public $lastError = '';

    /* @var int default consistency level */
    static private $_consistencyLevel = cassandra_ConsistencyLevel::ONE;

    /* @var string currently selected node */
    static private $_activePool = NULL;

    /* @var array available transports */
    static private $_socketPool = array();

    /* @var string currently selected node in a pool */
    static private $_activeNode = NULL;

    /* @var int maximum number of retries *per 'core'* before marking a host down */
    static private $_maxRetries = 2;

    /* @var int retry cooldown interval in seconds against a problem host */
    static private $_retryCooldown = 10;

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

    /* @var Memcached instance */
    static private $_memCached = NULL;

    /* @var bool APC is available for use */
    static private $_apcAvailable = FALSE;

    static private $_loggers = array();

    /* @var PandraCore instance of self */
    static private $_instance = NULL;

    /**
     *  dummy constructor
     */
    private function __construct() {
    }

    /**
     * Singleton instantiation of this object
     * @return PandraCore self instance
     */
    static public function getInstance() {
        if (NULL === self::$_instance) {
            $c = __CLASS__;
            self::$_instance = new $c;
        }
        return self::$_instance;
    }

    /**
     * Supported Modes accessor
     * @return array supported read/write modes
     */
    static public function getSupportedModes() {
        return self::$_supportedModes;
    }

    /**
     * readmode mutator
     * @param int $newMode new mode
     */
    static public function setReadMode($newMode) {
        if (!array_key_exists($newMode, self::$_supportedModes)) throw new RuntimeExcpetion("Unsupported Read Mode");
        self::$readMode = $newMode;
    }

    /**
     * readmode accessor
     * @return int read mode
     */
    static public function getReadMode() {
        return self::$readMode;
    }

    /**
     * readmode mutator
     * @param int $newMode new mode
     */
    static public function setWriteMode($newMode) {
        if (!array_key_exists($newMode, self::$_supportedModes)) throw new RuntimeExcpetion("Unsupported Write Mode");
        self::$writeMode = $newMode;
    }

    /**
     * readmode accessor
     * @return int read mode
     */
    static public function getWriteMode() {
        return self::$writeMode;
    }


    /**
     * Sets connectionid as active node
     * @param string $connectionID named connection
     * @return bool connection id exists and has been set
     */
    static public function setActiveNode($connectionID) {
        if (array_key_exists($connectionID, self::$_socketPool[self::$_activePool]) && self::$_socketPool[self::$_activePool][$connectionID]['transport']->isOpen()) {
            self::$_activeNode = $connectionID;
            return TRUE;
        }
        return FALSE;
    }

    /**
     * Sets connectionid as active node
     * @param string $connectionID named connection
     * @return bool connection id exists and has been set
     */
    static public function setActivePool($poolName) {
        if (array_key_exists($poolName, self::$_socketPool)) {
            self::$_activePool = $poolName;
            // grab last node in the pool to set active
            $poolNames = array_keys(self::$_socketPool[$poolName]);
            $connectionID = array_pop($poolNames);
            self::setActiveNode($connectionID);
            return TRUE;
        }
        return FALSE;
    }

    /**
     * Disconnects a named connection
     * @param string $connectionID named connection
     * @return bool disconnected OK
     */
    static public function disconnect($connectionID, $poolName = self::DEFAULT_POOL_NAME) {
        if (array_key_exists($connectionID, self::$_socketPool[self::$_activePool])) {
            if (self::$_socketPool[self::$_activePool][$connectionID]['transport']->isOpen()) {
                self::$_socketPool[self::$_activePool][$connectionID]['transport']->close();
                return TRUE;
            }
        }
        return FALSE;
    }

    /**
     * Disconnects all nodes in all pools
     * @return bool disconnected OK
     */
    static public function disconnectAll() {

        foreach (self::$_socketPool as $poolName => $socketPool) {
            $connections = array_keys($socketPool);

            foreach ($connections as $connectionID) {
                if (!self::disconnect($connectionID, $poolName)) throw new RuntimeException($connectionID.' could not be closed');
            }
        }
        return TRUE;
    }

    /**
     * Connects to given Cassandra node and makes it available in the static connection pool
     * @param string $connectionID named node within connection pool
     * @param string $host host name or IP of connecting node
     * @param string $poolName name of the connection pool (cluster name)
     * @param int $port TCP port of connecting node
     * @return bool connected ok
     */
    static public function connect($connectionID, $host, $poolName = self::DEFAULT_POOL_NAME, $port = THRIFT_PORT_DEFAULT) {
        try {

            // check connectionid hasn't been marked as down
            if (self::priorFail($connectionID)) {
                self::registerError($host.'/'.$port.' is marked DOWN', PandraLog::LOG_CRIT);
            } else {
                // if the connection exists but it is closed, then re-open
                if (array_key_exists($poolName, self::$_socketPool) && array_key_exists($connectionID, self::$_socketPool[$poolName])) {
                    if (!self::$_socketPool[$poolName][$connectionID]['transport']->isOpen()) {
                        self::$_socketPool[$poolName][$connectionID]['transport']->open();
                    }
                    return TRUE;
                }

                if (!array_key_exists($poolName, self::$_socketPool)) self::$_socketPool[$poolName] = array();

                // Create Thrift transport and binary protocol cassandra client
                $transport = new TBufferedTransport(new TSocket($host, $port, PERSIST_CONNECTIONS, 'PandraCore::registerError'), 1024, 1024);
                $transport->open();

                self::$_socketPool[$poolName][$connectionID] = array(
                        'retries' => 0,
                        'transport' => $transport,
                        'client' => new CassandraClient(
                        (PANDRA_64 &&
                                function_exists("thrift_protocol_write_binary") ?
                        new TBinaryProtocolAccelerated($transport) :
                        new TBinaryProtocol($transport)))
                );

                // set new connection the active, working master
                self::setActivePool($poolName);
                self::setActiveNode($connectionID);
                return TRUE;
            }
        } catch (TException $te) {
            self::registerError('TException: '.$te->getMessage(), PandraLog::LOG_CRIT);

        }

        return FALSE;
    }

    /**
     * Makes a named logger available to Core
     * @param string $loggerName Logger class name (minus PandraLogger Prefix)
     * @param array $params parameters to pass through to logger
     * @return bool Logger found and registered OK
     */
    static public function addLogger($loggerName, $params = array()) {
        if (!array_key_exists($loggerName, self::$_loggers)) {

            $registered = PandraLog::register($loggerName, $params);
            $logger = PandraLog::getLogger($loggerName);
            if ($registered && $logger !== NULL) {
                self::$_loggers[$loggerName] = $logger;
            } else {
                return FALSE;
            }
        }
        return TRUE;
    }

    /**
     * Drops a logger from the registered logger pool
     * @param string $loggerName Logger class name (minus PandraLogger Prefix)
     * @return bool Logger removed ok
     */
    static public function removeLogger($loggerName) {
        if (array_key_exists($loggerName, self::$_loggers)) {
            unset(self::$_loggers[$loggerName]);
            return TRUE;
        }
        return FALSE;
    }

    /**
     * Gets the instance of a named registered logger
     * @param string $loggerName Logger class name (minus PandraLogger Prefix)
     * @return PandraLogger Logger instance
     */
    static public function getLogger($loggerName) {
        if (array_key_exists($loggerName, self::$_loggers)) {
            return self::$_loggers[$loggerName];
        }
        return NULL;
    }

    /**
     * Adds an error message to Core's running log, and sends the message to any registered loggers
     * @param string $errorMsg Error Message
     * @param int $priority error priority level (PandraLog::LOG_)
     */
    static public function registerError($errorMsg, $priority = PandraLog::LOG_WARNING) {
        $message = '(PandraCore) '.$errorMsg;
        PandraLog::logPriorityMessage($priority, $message);
        self::$lastError = $errorMsg;
    }

    /**
     * Given a single host, attempts to find other nodes in the cluster and attaches them
     * to the pool
     * @todo build connections from token map
     * @param string $host host name or IP of connecting node
     * @param string $poolName name of the connection pool (cluster name)
     * @param int $port TCP port of connecting node
     * @return bool connected ok
     */
    static public function auto($host, $poolName = self::DEFAULT_POOL_NAME, $port = THRIFT_PORT_DEFAULT) {

        try {
            // Create Thrift transport and binary protocol cassandra client
            $transport = new TBufferedTransport(new TSocket($host, $port, PERSIST_CONNECTIONS, 'PandraCore::registerError'), 1024, 1024);
            $transport->open();
            $client = new CassandraClient(
                    (function_exists("thrift_protocol_write_binary") ?
                    new TBinaryProtocolAccelerated($transport) :
                    new TBinaryProtocol($transport)));

            $tokenMap = $client->get_string_property('token map');
            $tokens = json_decode($tokenMap);
            foreach ($tokens as $token => $host) {
                if (!self::connect($token, $host, $poolName)) {
                    return FALSE;
                }
            }

            return TRUE;
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
        }
        return FALSE;
    }

    /**
     * Gets a list of connnection id's for a given pool
     * @param <type> $poolName
     * @return <type>
     */
    static public function getPoolTokens($poolName = self::DEFAULT_POOL_NAME) {
        if (!empty(self::$_socketPool[$poolName])) {
            return array_keys(self::$_socketPool[$poolName]);
        }
        return array();
    }

    /**
     * memcached mutator
     * @param bool memcached is available
     */
    static public function setMemcachedAvailable($memcachedAvailable) {
        self::$_memcachedAvailable = $memcachedAvailable;
    }

    /**
     * memcached accessor
     * @return bool memcached has been detected
     */
    static public function getMemcachedAvailable() {
        return self::$_memcachedAvailable;
    }

    /**
     * Binds a Memcached instance to core for use
     * @param Memcached $memcached
     */
    static public function bindMemcached(Memcached $memcached) {
        self::$_memCached = $memcached;
        self::setMemcachedAvailable(TRUE);
    }

    /**
     * apc mutator
     * @param bool apc is available
     */
    static public function setAPCAvailable($apcAvailable) {
        self::$_apcAvailable = $apcAvailable;
    }

    /**
     * apc available accessor
     * @return bool apc has been detected
     */
    static public function getAPCAvailable() {
        return self::$_apcAvailable;
    }

    /**
     * get current working node, recursive, trims disconnected clients
     */
    static public function getClient($writeMode = FALSE) {

        // Catch trimmed nodes or a completely trimmed pool
        if (empty(self::$_activeNode) || empty(self::$_socketPool[self::$_activePool])) {
            self::registerError('Not Connected', PandraLog::LOG_CRIT);
            throw new Exception('Not Connected');
        }

        $activePool = self::$_socketPool[self::$_activePool];

        $useMode = ($writeMode) ? self::$writeMode : self::$readMode;
        switch ($useMode) {
            case self::MODE_ROUND :
                if (!current(self::$_socketPool[self::$_activePool])) reset(self::$_socketPool[self::$_activePool]);
                $curConn = each(self::$_socketPool[self::$_activePool]);
                self::$_activeNode = $curConn['key'];		// store current working node
                $conn = self::$_socketPool[self::$_activePool][self::$_activeNode]['client'];
                break;

            case self::MODE_RANDOM :
                self::$_activeNode = array_rand($activePool);
                $conn = self::$_socketPool[self::$_activePool][self::$_activeNode]['client'];
                break;

            case self::MODE_ACTIVE :
            default :
            // If we're trying to use an explicit connection id and it's down, then bail
                if (self::priorFail(self::$_activeNode)) {
                    return NULL;
                }

                $conn = self::$_socketPool[self::$_activePool][self::$_activeNode]['client'];
                break;
        }

        // check connection is open
        try {
            self::$_socketPool[self::$_activePool][self::$_activeNode]['transport']->open();
            return $conn;
        } catch (TException $te) {

            if (++self::$_socketPool[self::$_activePool][self::$_activeNode]['retries'] > self::$_maxRetries) {
                self::setLastFail();
                unset(self::$_socketPool[self::$_activePool][self::$_activeNode]);
            }

            self::registerError(self::$_activePool.':'.self::$_activeNode.': Marked as DOWN, trying next in pool');
            return self::getClient($writeMode);
        }
    }

    /**
     * Stores the last failure time for a node in the cache (Memcached takes precedence)
     * @param string $connectionID connection id
     */
    static private function setLastFail($connectionID = NULL) {
        $key = 'lastfail_'.md5($connectionID === NULL ? self::$_activeNode : $connectionID);
        if (self::$_memcachedAvailable && self::$_memCached instanceof Memcached) {
            self::$_memcached->set($key, time(), self::$_retryCooldown);
        } elseif (self::$_apcAvailable) {
            apc_store($key, time(), self::$_retryCooldown);
        }
    }

    /**
     * Checks in the cache if the node has been marked as down (Memcached takes precedence)
     * @param string $connectionID connection id
     * @return bool node marked down
     */
    static private function priorFail($connectionID = NULL) {
        $key = 'lastfail_'.md5($connectionID === NULL ? self::$_activeNode : $connectionID);
        $ok = FALSE;
        if (self::$_memcachedAvailable && self::$_memCached instanceof Memcached) {
            $result = is_numeric(self::$_memcached->get($key));
            return $result;
        } elseif (self::$_apcAvailable) {
            // relying on the retry
            $result = is_numeric(apc_fetch($key, $ok));
            return $ok && $result;
        }
        return $ok;
    }

    /**
     * Returns description of keyspace
     * @param string $keySpace keyspace name
     * @return array keyspace structure
     */
    static public function describeKeyspace($keySpace) {
        $client = self::getClient();
        return $client->describe_keyspace($keySpace);
    }

    /**
     * consistency accessor
     * @param int $consistencyLevel overrides the return, or returns default if NULL
     * @return int consistency level
     */
    static public function getConsistency($consistencyLevel = NULL) {
        $consistency = self::$_consistencyLevel;
        if ($consistencyLevel !== NULL) {
            $consistency = $consistencyLevel;
        }

        return $consistency;
    }

    /**
     * consistency mutator
     * @param int $consistencyLevel new consistency level
     */
    static public function setConsistency($consistencyLevel) {
        self::$_consistencyLevel = $consistencyLevel;
    }

    /**
     * Loads the local cassandra conf file for use
     * @return SimpleXMLElement SimpleXML config structure
     */
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
        // @todo patch thrift .so
        if (PANDRA_64 || (!PANDRA_64 && !function_exists("thrift_protocol_write_binary"))) {
            return round(microtime(true) * 1000, 3);
        }
        return time();
    }

    // ----------------------- THRIFT COLUMN PATH INTERFACE

    /**
     * Deletes a Column Path from Cassandra
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnPath $columnPath
     * @param int $time deletion timestamp
     * @param int $consistencyLevel response consistency level
     * @return bool Column Path deleted OK
     */
    static  public function deleteColumnPath($keySpace,
            $keyID,
            cassandra_ColumnPath $columnPath,
            $time = NULL,
            $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);
            if ($time === NULL) {
                $time = self::getTime();
            }
            $client->remove($keySpace, $keyID, $columnPath, $time, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
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
    static public function saveColumnPath($keySpace,
            $keyID,
            cassandra_ColumnPath $columnPath,
            $value,
            $time = NULL,
            $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);
            if ($time === NULL) {
                $time = self::getTime();
            }
            $client->insert($keySpace, $keyID, $columnPath, $value, $time, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Batch saves a SuperColumn and its modified Columns to Cassandra
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param string $superCFName Super Column Family name
     * @param array $superColumnMap array of Super Column name (string) keyed to array of modified cassandra_Columns
     * @param int $consistencyLevel response consistency level
     * @return bool Super Column saved OK
     */
    static public function saveSuperColumn($keySpace,
            $keyID,
            array $superCFName,
            array $superColumnMap,
            $consistencyLevel = NULL) {
        try {
            $client = self::getClient(TRUE);

            $mutations = array();

            foreach ($superCFName as $superColumnFamilyName) {

                // Thrift won't batch insert multiple supercolumns?
                $scContainer = new cassandra_ColumnOrSuperColumn();

                foreach ($superColumnMap as $superColumnName => $columns) {
                    $scContainer->super_column = new cassandra_SuperColumn();
                    $scContainer->super_column->name = $superColumnName;
                    $scContainer->super_column->columns = $columns;
                }

                $mutations[$superColumnFamilyName] = array($scContainer);
            }

            // batch_insert inserts a supercolumn across multiple CF's for key
            $client->batch_insert($keySpace, $keyID, $mutations, self::getConsistency($consistencyLevel));

        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
            return FALSE;
        }
        return TRUE;
    }

    /**
     * Gets complete slice of Thrift cassandra_Column objects for keyID
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnParent $columnParent
     * @param cassandra_SlicePredicate $predicate column names or range predicate
     * @param int $consistencyLevel response consistency level
     * @return cassandra_Column Thrift cassandra column
     */
    static public function getCFSlice($keySpace,
            $keyID,
            cassandra_ColumnParent $columnParent,
            cassandra_SlicePredicate $predicate,
            $consistencyLevel = NULL) {

        $client = self::getClient();

        try {
            return $client->get_slice($keySpace, $keyID, $columnParent, $predicate, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
            return NULL;
        }
        return NULL;
    }

    /**
     * Retrieves slice of columns across keys in parallel
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnParent $columnParent
     * @param cassandra_SlicePredicate $predicate column names or range predicate
     * @param int $consistencyLevel response consistency level
     * @return array keyed array of matching cassandra_ColumnOrSuperColumn objects
     */
    static public function getCFSliceMulti($keySpace,
            array $keyIDs,
            cassandra_ColumnParent $columnParent,
            cassandra_SlicePredicate $predicate,
            $consistencyLevel = NULL) {

        $client = self::getClient();

        try {
            return $client->multiget_slice($keySpace, $keyIDs, $columnParent, $predicate, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
            return NULL;
        }
    }

    /**
     * Returns by key, the column count in a column family (or a single CF/SuperColumn pair)
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnParent $columnParent
     * @param int $consistencyLevel response consistency level
     * @return int number of rows or null on error
     */
    static public function getCFColumnCount($keySpace,
            $keyID,
            cassandra_ColumnParent
            $columnParent,
            $consistencyLevel = NULL) {

        $client = self::getClient();

        try {
            return $client->get_count($keySpace, $keyID, $columnParent, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
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
     * @param int $consistencyLevel response consistency level
     * @return cassandra_Column Thrift cassandra column
     */
    static public function getColumn($keySpace,
            $keyID,
            $columnFamilyName,
            $columnName,
            $superColumnName = NULL,
            $consistencyLevel = NULL) {

        $columnPath = new cassandra_ColumnPath(array(
                        'column_family' => $columnFamilyName,
                        'super_column' => $superColumnName,
                        'column' => $columnName
        ));

        return self::getColumnPath($keySpace, $keyID, $columnPath, $consistencyLevel);
    }

    /**
     * @param string $keySpace keyspace of key
     * @param string $keyID row key id
     * @param cassandra_ColumnPath $columnPath
     * @param int $consistencyLevel response consistency level
     * @return cassandra_Column
     */
    static public function getColumnPath($keySpace,
            $keyID,
            cassandra_ColumnPath
            $columnPath,
            $consistencyLevel = NULL) {
        $client = self::getClient();

        try {
            return $client->get($keySpace, $keyID, $columnPath, self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
            return NULL;
        }
    }

    /**
     * @param string $keySpace keyspace of key
     * @param array $keyRange associative array of keys indexed by 'start' and 'finish'
     * @param cassandra_ColumnParent $columnParent
     * @param cassandra_SlicePredicate $predicate column names or range predicate
     * @param int number of rows to return
     * @param int $consistencyLevel response consistency level
     * @return <type>
     */
    static public function getRangeKeys($keySpace,
            array $keyRange,
            cassandra_ColumnParent $columnParent,
            cassandra_SlicePredicate $predicate,
            $numRows = DEFAULT_ROW_LIMIT,
            $consistencyLevel = NULL) {

        $client = self::getClient();

        try {
            return $client->get_range_slice($keySpace,
                    $columnParent,
                    $predicate,
                    $keyRange['start'],
                    $keyRange['finish'],
                    $numRows,
                    self::getConsistency($consistencyLevel));
        } catch (TException $te) {
            self::registerError( 'TException: '.$te->getMessage().' '.(isset($te->why) ? $te->why : ''));
            return NULL;
        }
    }
}

// Setup our capabilities
PandraCore::setMemcachedAvailable(class_exists('Memcached'));

PandraCore::setAPCAvailable(function_exists('apc_sma_info') && apc_sma_info() !== FALSE);
?>