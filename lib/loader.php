<?php
/**
 * Loader
 *
 * Handles autoloading for the Pandra package and calls Core static construction methods
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.2
 * @package pandra
 */
define('PANDRA_64', PHP_INT_SIZE == 8);
function _pandraAutoLoad($className) {
    // seperate classes and interfaces for clarity
    $fExt = array('.class.php', '.interface.php');

    // strip prefix
    $className = preg_replace('/^pandra/i', '', $className);

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
spl_autoload_register('_pandraAutoLoad');

// Setup our capabilities
PandraCore::setMemcachedAvailable(class_exists('Memcached'));
PandraCore::setAPCAvailable(function_exists('apc_sma_info') && apc_sma_info() !== FALSE);
?>