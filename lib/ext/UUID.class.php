<?php
/**
 * Generates v1 (Timestamp) and v5 (Lexical, SHA-1) UUID's for use with Cassandra
 *
 * Core is a gracefully degrading static connection manager, Thrift API helper and
 * cache manager
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @author Marius Karthaus
 * @link http://www.php.net/manual/en/function.uniqid.php#88434
 */
class UUID {

    public static $_uuid;

    private static function instance() {
        if (!is_resource(self::$_uuid)) {
            uuid_create(&self::$_uuid);
        }
        return self::$_uuid;
    }

    public static function generate() {
        return self::v1();
    }

    /**
     * returns a type 1 (MAC address and time based) uuid
     * @return string
     */
    public static function v1() {
        return self::_generate(UUID_MAKE_V1);
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

    public static function convert($uuid, $toFmt) {
        $uuidConv = $uuid;
        $fromFmt = self::isBinary($uuid) ? UUID_FMT_BIN : UUID_FMT_STR;
        uuid_import(&self::$_uuid, $fromFmt, $uuid);
        uuid_export(self::instance(), $toFmt, &$uuidConv);
        return $uuidConv;
    }

    public static function validUUID($uuidStr) {
        return preg_match('/^\{?[0-9a-f]{8}\-?[0-9a-f]{4}\-?[0-9a-f]{4}\-?[0-9a-f]{4}\-?[0-9a-f]{12}\}?$/i', $uuidStr);
    }

    public static function isBinary($uuid) {
        return preg_match('/((?![\x20-\x7E]).)/', $uuid);
    }
}
?>