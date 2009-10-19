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

The PTK homepage is :
http://www.phpgrease.net/projects/ptk
*/

$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__).'/../thrift-php/';

require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

define('PTK_LOG_SYSLOG', 1);
define('PTK_LOG_FIREPHP', 2);
define('PTK_LOG_ALL', 3);

define('PTK_LOGGING', PTK_LOG_ALL);
define('PTK_PORT_DEFAULT', 9160);

function ptkAutoLoad($className) {
    $className = strtolower($className);
    if (!preg_match("/^ptk/", $className)) return;

    // class path relative to config
    $classPath = dirname(__FILE__)."/lib/";

    // class file suffix
    $cSuffix = ".class.php";

    if (preg_match("/^ptk_/", $className)) {
        list(, $keySpace, $cFamily) = preg_split("/\_/", $className);
        if (!empty($keySpace) && !empty($cFamily)) {
            // maps to classes/keyspace/columnfamily
            $classPath .= ptkcache::cacheDIR."/".$keySpace."/";
        }
    }

    $classFile = $classPath.$className.$cSuffix;
    if (file_exists($classFile)) {
        require_once($classFile);
    }
}
spl_autoload_register('ptkAutoload');
