<?php
/**
 * @todo read/write switching, memcached hooks, logging hooks
 */
class ptk {

	static public $lastError = '';

	static private $_nodeConns = array();
	static private $_activeNode = NULL;
	static private $consistencyLevel = cassandra_ConsistencyLevel::ZERO;

    /**
     * 
     */
	public function setactiveNode($connectionID) {
		if (array_key_exists($connectionID, self::$_nodeConns)) {
			self::$_activeNode = $connectionID;
		}
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
        while ($conn = each(self::$_nodeConns)) {
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
	static public function connect($connectionID, $host, $port = PTK_PORT_DEFAULT) {
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
     * get current working node
     */
    static public function getClient() {
        return self::$_nodeConns[self::$_activeNode]['client'];
    }

    static public function getKeyspace($keySpace) {
        $client = self::getClient();
        return $client->describe_keyspace($keySpace);
    }
}