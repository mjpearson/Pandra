<?
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/thrift-php/';

require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

// Config xml path for Cassandra
define('CASSANDRA_CONF_PATH', '/usr/local/src/apache-cassandra-incubating-0.5.0/conf/storage-conf.xml');

// Default Thrift port
define('PANDRA_PORT_DEFAULT', 9160);

// read/write modes (can be configured independently)
define('PANDRA_MODE_ACTIVE', 0);	// Active client only
define('PANDRA_MODE_ROUND', 1);		// sequentially select configured clients 
define('PANDRA_MODE_ROUND_APC', 1);	// sequentially select between interpreter instances w/APC
define('PANDRA_MODE_RANDOM', 2);	// select random node

// Column Family/Column Types (Standard or Super)
define('PANDRA_STANDARD', 0);
define('PANDRA_SUPER', 1);

define('PANDRA_DEFAULT_CREATE_MODE', TRUE);

define('PANDRA_DEFAULT_CONSISTENCY', cassandra_ConsistencyLevel::ONE);


function _pandraAutoLoad($className) {

    	if (!preg_match("/^pandra/i", $className)) return;

	if ($className != 'Pandra') $className = preg_replace('/^pandra/i', '', $className);

    	// class path relative to config
    	$classPath = dirname(__FILE__)."/lib/";

    	// class file suffix
    	$cSuffix = ".class.php";

    	$classFile = $classPath.$className.$cSuffix;
    	if (file_exists($classFile)) {
        	require_once($classFile);
	}
}
spl_autoload_register('_pandraAutoload');
?>