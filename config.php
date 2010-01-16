<?
/*
Copyright (C) 2009 PHPGrease.net

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

The Pandra homepage is :
http://www.phpgrease.net/projects/pandra
*/
/**
 *
 * @package Pandra
 */
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/../thrift-php/';

require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

// Config xml path for Cassandra
define('CASSANDRA_CONF_PATH', '/usr/local/src/apache-cassandra-incubating-0.4.0/conf/storage-conf.xml');

// Default Thrift port
define('PANDRA_PORT_DEFAULT', 9160);

// read/write modes (can be configured independently)
define('PANDRA_MODE_ACTIVE', 0);	// Active client only
define('PANDRA_MODE_ROUND', 1);		// sequentially select configured clients 
define('PANDRA_MODE_ROUND_APC', 1);	// sequentially select between interpreter instances w/APC
define('PANDRA_MODE_RANDOM', 2);	// select random node

// Column Family Types
define('PANDRA_CF_STANDARD', 0);
define('PANDRA_CF_SUPER', 1);

function pandraAutoLoad($className) {

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

spl_autoload_register('pandraAutoload');
