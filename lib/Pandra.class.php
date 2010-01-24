<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */
class Pandra {

    /*
    const FORMAT_OBJ = 1;
    const FORMAT_ASSOC = 2;
    const FORMAT_XML = 3;
    const FORMAT_JSON = 4;

    const APC_EXPIRE_SECONDS = 60;
    */

    static public $lastError = '';

    static public $consistencyLevel = cassandra_ConsistencyLevel::ZERO;

    static private $_nodeConns = array();

    static private $_activeNode = NULL;

    static private $readMode = PANDRA_MODE_ACTIVE;

    static private $writeMode = PANDRA_MODE_ACTIVE;

    static private $_supportedModes = array(
            PANDRA_MODE_ACTIVE,
            PANDRA_MODE_ROUND,
            //PANDRA_MODE_ROUND_APC,
            PANDRA_MODE_RANDOM,
    );

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
    static public function connect($connectionID, $host = NULL, $port = PANDRA_PORT_DEFAULT) {
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
                    'client' => new CassandraClient(new TBinaryProtocol($transport))
            );

            // set new connection the active, working master
            self::setActiveNode($connectionID);
            return TRUE;
        } catch (TException $tx) {
            self::$lastError = 'TException: '.$tx->getMessage() . "\n";
        }
       return FALSE;

    }

    /**
     * get current working node, recursive, trims disconnected clients
     */
    static public function getClient($writeMode = FALSE) {
        if (empty(self::$_activeNode)) throw new Exception('Not Connected');
        $useMode = ($writeMode) ? self::$writeMode : self::$readMode;
        switch ($useMode) {
            case PANDRA_MODE_ROUND_APC :
            // @todo, APC store of activeNode
            case PANDRA_MODE_ROUND :
                if (!current(self::$_nodeConns)) reset(self::$_nodeConns);
                $curConn = each(self::$_nodeConns);
                self::$_activeNode = $curConn['key'];		// store current working node
                return self::$_nodeConns[self::$_activeNode]['client'];
                break;
            case PANDRA_MODE_RANDOM :
                $randConn =& array_rand(self::$_nodeConns);
                return self::$_nodeConns[$randConn]['client'];
                break;
            case PANDRA_MODE_ACTIVE :
            default :
                return self::$_nodeConns[self::$_activeNode]['client'];
                break;
        }
    }

    static public function getKeyspace($keySpace) {
        $client = self::getClient();
        return $client->describe_keyspace($keySpace);
    }

    static public function loadConfigXML() {
        if (!file_exists(CASSANDRA_CONF_PATH)) {
            throw new RuntimeException('Cannot build models, file not found ('.CASSANDRA_CONF_PATH.')\n');
        }

        $conf = simplexml_load_file(CASSANDRA_CONF_PATH);
        return $conf;
    }

    public function deleteColumnPath($keySpace, $keyID, cassandra_ColumnPath $columnPath, $time, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        try {
            $client = Pandra::getClient(TRUE);
            $client->remove($keySpace, $keyID, $columnPath, $time, $consistencyLevel);
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return FALSE;
        }
        return TRUE;
    }

    public function saveColumnPath($keySpace, $keyID, cassandra_ColumnPath $columnPath, $value,  $time, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {
        try {
            $client = Pandra::getClient(TRUE);
            $client->insert($keySpace, $keyID, $columnPath, $value, $time, $consistencyLevel);
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
    public function getCFSlice($keyID, $keySpace, $cfName, $superName = NULL, $consistencyLevel = cassandra_ConsistencyLevel::ONE) {

        $client = Pandra::getClient();

        // build the column path
        $columnParent = new cassandra_ColumnParent();
        $columnParent->column_family = $cfName;
        $columnParent->super_column = $superName;

        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = new cassandra_SliceRange();
        $predicate->slice_range->start = '';
        $predicate->slice_range->finish = '';

        try {
            if (is_array($keyID)) {
                return $client->multiget_slice($keySpace, $keyID, $columnParent, $predicate, $consistencyLevel);
            } else {
                return $client->get_slice($keySpace, $keyID, $columnParent, $predicate, $consistencyLevel);
            }
        } catch (TException $te) {
            self::$lastError = 'TException: '.$te->getMessage() . "\n";
            return NULL;
        }
        return NULL;
    }





    /*
    static public function buildModels($ks = NULL) {
        $conf = self::loadConfigXML();

        foreach ($conf->Keyspaces as $keySpace) {

            var_dump($keySpace);

            $ksName = $keySpace->attributes()->Name;
            var_dump($ksName);

            $columnFamilies = $keySpace->xpath('Keyspace/ColumnFamily');
            var_dump($columnFamilies);

      //  var_dump($keySpace);
        }

    }

    static public function toJSON(&$results) {
    }

    static public function toXML(&$results) {
    }

    static public function toSerialised(&$results) {
    }

    static public function toArray(&$results) {
    }
    */
}
?>