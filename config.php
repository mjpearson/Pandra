<?php
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

function _pandraAutoLoad($className) {

    if (!preg_match("/^pandra/i", $className)) return;

    $className = preg_replace('/^pandra/i', '', $className);

    // class path relative to config
    $classPath = dirname(__FILE__)."/lib/";

    // class file suffix
    $cSuffix = ".class.php";

    $classFile = $classPath.$className.$cSuffix;

    if (file_exists($classFile)) {
        require_once($classFile);
    }
}
spl_autoload_register('_pandraAutoLoad');
?>
