<?php
/**
 * @package Pandra
 */
class Pandra {

	const FORMAT_OBJ = 1;
	const FORMAT_ASSOC = 2;
	const FORMAT_XML = 3;
	const FORMAT_JSON = 4;

	//const APC_EXPIRE_SECONDS = 60;

	static public $lastError = '';
	static public $consistencyLevel = cassandra_ConsistencyLevel::ZERO;

	static private $_nodeConns = array();
	static private $_activeNode = NULL;

	static private $readMode = PANDRA_MODE_ACTIVE;
	static private $writeMode = PANDRA_MODE_ACTIVE;

	static private $supportedModes = array(
						PANDRA_MODE_ACTIVE,
						PANDRA_MODE_ROUND,
						//PANDRA_MODE_ROUND_APC,
						PANDRA_MODE_RANDOM,
						);

	static public function setReadMode($newMode) {
		if (!array_key_exists($newMode, self::$supportedModes)) throw new RuntimeExcpetion("Unsupported Read Mode");
		self::$readMode = $newMode;
	}

	static public function setWriteMode($newMode) {
		if (!array_key_exists($newMode, self::$supportedModes)) throw new RuntimeExcpetion("Unsupported Write Mode");
		self::$writeMode = $newMode;
	}


    /**
     * 
     */
	public function setActiveNode($connectionID) {
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
            }
        }
    }

    /**
     * 
     */
    static public function disconnectAll() {
	while ($connectionID = each(self::$_nodeConns)) {
		self::disconnect($connectionID);
	}
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
			return FALSE;
		}
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
			return $randConn['client'];
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

	static public function toJSON(&$results) {
	}

	static public function toXML(&$results) {
	}

	static public function toSerialised(&$results) {
	}

	static public function toArray(&$results) {
	}

}
