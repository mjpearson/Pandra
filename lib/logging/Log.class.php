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

    static public $priorityMap = array(
            self::LOG_EMERG => 'emergency',
            self::LOG_ALERT => 'alert',
            self::LOG_CRIT => 'critical',
            self::LOG_ERR => 'error',
            self::LOG_WARNING => 'warning',
            self::LOG_NOTICE => 'notice',
            self::LOG_INFO => 'info',
            self::LOG_DEBUG => 'debug',
    );

    static private function getClassFromName($loggerName) {
        return 'PandraLogger'.ucfirst($loggerName);
    }

    static public function getLogger($loggerName) {
        $lc = self::getClassFromName($loggerName);
        if (array_key_exists($lc, self::$_loggers)) return self::$_loggers[$lc];
        return NULL;
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

    static public function getRegisteredLoggers() {
        return array_values(self::$_loggers);
    }

    static public function isRegistered($loggerName) {
        $lc = self::getClassFromName($loggerName);
        return array_key_exists($lc, self::$_loggers);
    }

    static public function hasPriorityLogger($priority) {
        foreach (self::$_loggers as $logger) {
            if ($logger->isPriorityLogger($priority)) return TRUE;
        }
        return FALSE;
    }

    static public function logPriorityMessage($priority, $message) {
        foreach (self::$_loggers as $logger) {
            if ($logger->isPriorityLogger($priority)) $logger->execute($priority, $message);
        }
    }

    /* ------- Priority Helper */
    /*
     * @todo 5.3, strip the priority helpers and use callStatic
    static public function __callStatic($class, $args) {
        $priority = array_search(strtolower($class), self::$priorityMap);
        if (!empty($priority)) {
            self::logPriorityMessage($priority, array_pop($args));
        }
    }
    */

    static public function emerg($message) {
        self::logPriorityMessage(self::LOG_EMERG, $message);
    }

    static public function alert($message) {
        self::logPriorityMessage(self::LOG_ALERT, $message);
    }

    static public function crit($message) {
        self::logPriorityMessage(self::LOG_CRIT, $message);
    }

    static public function err($message) {
        self::logPriorityMessage(self::LOG_ERR, $message);
    }

    static public function warning($message) {
        self::logPriorityMessage(self::LOG_WARNING, $message);
    }

    static public function notice($message) {
        self::logPriorityMessage(self::LOG_NOTICE, $message);
    }

    static public function info($message) {
        self::logPriorityMessage(self::LOG_INFO, $message);
    }

    static public function debug($message) {
        self::logPriorityMessage(self::LOG_DEBUG, $message);
    }

}
?>