<?php

class PandraLog {

    const LOG_EMERG = 0;
    const LOG_ALERT = 1;
    const LOG_CRIT = 2;
    const LOG_ERR = 3;
    const LOG_WARNING = 4;
    const LOG_NOTICE = 5;
    const LOG_INFO = 6;
    const LOG_DEBUG = 7;

    static private $_loggers = array();

    static public $priorityStr = array(
        self::LOG_EMERG => 'Emergency',
        self::LOG_ALERT => 'Alert',
        self::LOG_CRIT => 'Critical',
        self::LOG_ERR => 'Error',
        self::LOG_WARNING => 'Warning',
        self::LOG_NOTICE => 'Notice',
        self::LOG_INFO => 'Info',
        self::LOG_DEBUG => 'Debug',
    );

    static private function getClassFromName($loggerName) {
        return 'PandraLogger'.ucfirst($loggerName);
    }

    static public function getLogger($loggerName) {
        $lc = self::getClassFromName($loggerName);
        if (array_key_exists($lc, self::$_loggers)) return self::$_loggers[$lc];
        return NULL;
    }

    static public function getRegisteredLoggers() {
        return array_values(self::$_loggers);
    }

    static public function register($loggerName, $params = array()) {
        $lc = self::getClassFromName($loggerName);
        if (!array_key_exists($lc, self::$_loggers)) {
            $lObj = new $lc($params);
            self::$_loggers[$lc] = &$lObj;
            return TRUE;
        }
        return FALSE;
    }

    static public function logTo($loggerName, $message, $priority = self::LOG_NOTICE) {
        $lc = self::getClassFromName($loggerName);
        if (array_key_exists($lc, self::$_loggers)) {
            self::$_loggers[$lc]->execute($priority, $message);
        } else {
            throw new RuntimeException("Logger '$lc' has not been registered");
        }
    }

    static public function isRegistered($loggerName) {
        $lc = self::getClassFromName($loggerName);
        return array_key_exists($lc, self::$_loggers);
    }

}
?>
