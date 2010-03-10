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

define('THRIFT_PORT_DEFAULT', 9160);

// nasty autoloader in the absense of namespace
function _pandraAutoLoad($className) {

    // just bail if it doesn't look like us
    if (!preg_match("/^pandra/i", $className)) return;

    // seperate classes and interfaces for clarity
    $fExt = array('.class.php', '.interface.php');

    // strip prefix
    $className = preg_replace('/^pandra/i', '', $className);

    // class path relative to config
    $classPath = dirname(__FILE__)."/lib/";

    if (preg_match('/^(Query|Clause)/', $className)) {
        $classPath .= 'query/';
    } elseif (preg_match('/^Log/', $className)) {
        $classPath .= 'logging/';
    }

    foreach ($fExt as $ext) {
        $classFile = $classPath.$className.$ext;
        if (file_exists($classFile)) {
            require_once($classFile);
            break;
        }
    }
}
spl_autoload_register('_pandraAutoLoad');
?>
