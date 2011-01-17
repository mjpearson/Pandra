<?php
/**
 * Loader
 *
 * Handles autoloading for the Pandra package and calls Core static construction methods
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010-2011 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.3
 * @package pandra
 */
namespace Pandra;

$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/thrift/';
require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

define('PANDRA_64', PHP_INT_SIZE == 8);
define('PANDRA_REQUEST_MICRO', round(microtime(true) * 1000000, 3));
define('PANDRA_INSTALL_DIR', dirname(__FILE__));

function _pandraAutoLoad($className) {

    if (!preg_match("/^".__NAMESPACE__."/i", $className)) return;

    $className = str_replace(__NAMESPACE__ .'\\' , '', $className);

    // seperate classes and interfaces for clarity
    $fExt = array('.class.php', '.interface.php');

    // class path relative to config
    $classPath = dirname(__FILE__)."/";

    if (preg_match('/^(Query|Clause)/', $className)) {
        $classPath .= 'query/';
    } elseif (preg_match('/^Log/', $className)) {
        $classPath .= 'logging/';
    } elseif (preg_match('/^UUID/', $className)) {
        $classPath .= 'uuid/';
    }

    foreach ($fExt as $ext) {
        $classFile = $classPath.$className.$ext;
        if (file_exists($classFile)) {
            require_once($classFile);
            break;
        }
    }
}
set_include_path(get_include_path() . PATH_SEPARATOR . __DIR__);

//spl_autoload_extensions(".class.php, .interface.php");
spl_autoload_register(__NAMESPACE__ . '\_pandraAutoLoad');


// Setup our capabilities
Core::setMemcachedAvailable(class_exists('Memcached'));
Core::setAPCAvailable(function_exists('apc_sma_info') && apc_sma_info() !== FALSE);

UUID::auto();