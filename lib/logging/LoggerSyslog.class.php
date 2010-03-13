<?php
class PandraLoggerSyslog implements PandraLogger {

    private $_isOpen = FALSE;

    private $_maxPriority = PandraLog::LOG_ERR;

    public function __construct(array $params) {
        if (empty($params)) {
            $params = array('ident' => 'pandra', 'option' => LOG_ODELAY, 'facility' => LOG_SYSLOG);
        }
        if (function_exists('openlog')) {
            return $this->open($params['ident'], $params['option'], $params['facility']);
        }
        return FALSE;
    }

    public function isOpen() {
        return $this->_isOpen;
    }

    public function open($ident, $option = LOG_ODELAY, $facility = LOG_SYSLOG) {
        $this->_isOpen = openlog($ident, $option, $facility);
        return $this->_isOpen;
    }

    /**
     * Syslog shouldn't handle notices, info's etc.
     * @param int $priority requested priority log
     * @return boolean this logger will log for priority
     */
    public function isPriorityLogger($priority) {
        return ($priority <= $this->_maxPriority);
    }

    public function execute($priority, $message) {
        if ($this->isPriorityLogger($priority) &&
                !empty($message)
                && $this->_isOpen) {

            if (is_array($message)) {
                foreach ($message as $msg) {
                    syslog($priority, $message);
                }
            } else {
                syslog($priority, $message);
            }
            return TRUE;
        }
        return FALSE;
    }

    public function close() {
        if ($this->_isOpen) return closelog();
        return FALSE;
    }

    public function  __destruct() {
        $this->close();
    }

}
?>
