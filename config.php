<?
/*
Panda Toolkit (PTK) - abstractor for Cassandra/PHP

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

The PTK homepage is :
http://github.com/sliggles/Panda-Toolkit
*/

define('PTK_FB_AVAILABLE', (int) class_exists('FB'));
define('PTK_FB_ENABLED', PTK_FB_AVAILABLE & 1);

function PTKAutoLoad() {
	$className = strtolower($className);
        $classPath = dirname(__FILE__)."/";

        // class suffix
        $cSuffix = ".class.php";

        // -- Database Layer
        if (preg_match("/^PTK/", $className)) {
                $classPath .= '/';

                // table abstractions
                if (preg_match("/^PTK_/", $className)) {
                        list(, $cdb, $ctable) = preg_split("/\_/", $className);
                        if (!empty($cdb) && !empty($ctable)) {
                                // maps to classes/db/dbName
                                $classPath .= PTKCache::cacheDIR."/".$cdb."/";
                        }
                }
        }

        $classFile = $classPath.$className.$cSuffix;
        if (file_exists($classFile)) {
        	require_once($classFile);
        }	
}

spl_autoload_register('PTKAutoload');
