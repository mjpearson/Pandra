<?php
/**
 * OSSP UUID plugin
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.2
 * @package pandra
 */
class PandraUUIDPluginOSSP implements PandraUUIDPlugin {

    private static $_uuid;

    public static function isCapable() {
        return (defined('UUID_MAKE_V1') && function_exists('uuid_create'));
    }

    private static function instance() {
        if (!is_resource(self::$_uuid)) {
            uuid_create(&self::$_uuid);
        }
        return self::$_uuid;
    }

    public static function generate() {
        return self::_generate(UUID_MAKE_V1);
    }

    /**
     * returns a type 1 (MAC address and time based) uuid
     * @return string
     */
    public static function v1() {
        return self::_generate(UUID_MAKE_V1);
    }

    /**
     * returns a type 4 (random) uuid
     * @return string
     */
    public static function v4() {
        return self::_generate(UUID_MAKE_V4);
    }

    /**
     * returns a type 5 (SHA-1 hash) uuid
     * @return string
     */
    public static function v5() {
        return self::_generate(UUID_MAKE_V5);
    }

    private static function _generate($type) {
        uuid_make ( self::instance(), $type );
        uuid_export ( self::instance(), UUID_FMT_STR, &$uuidstring );
        return trim ( $uuidstring );
    }
}
?>