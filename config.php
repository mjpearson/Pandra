<?php
/**
 * Config
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.2
 * @package pandra
 */
//error_reporting(E_ALL);
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/thrift-php/';
require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

require_once dirname(__FILE__).'/lib/loader.php';

define('MODEL_OUT_DIR', dirname(__FILE__).'/models/');
define('SCHEMA_PATH', dirname(__FILE__).'/schemas/');
define('THRIFT_PORT_DEFAULT', 9160);
define('DEFAULT_ROW_LIMIT', 10);
define('PERSIST_CONNECTIONS', FALSE); // TSocket Persistence
define('CASSANDRA_CONF_PATH', '/usr/local/src/apache-cassandra-0.6.1/conf/storage-conf.xml');
?>