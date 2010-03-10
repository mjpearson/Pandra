<?php
class PandraLoggerSyslog implements PandraLogger {

    private $_isOpen = FALSE;

    public function __construct(array $params) {
        if (empty($params)) {
            $params = array('ident' => 'pandra', 'option' => LOG_ODELAY, 'facility' => LOG_SYSLOG);
        }

        return $this->open($params['ident'], $params['option'], $params['facility']);
    }

    public function isOpen() {
        return $this->_isOpen;
    }

    public function open($ident, $option = LOG_ODELAY, $facility = LOG_SYSLOG) {
        $this->_isOpen = openlog($ident, $option, $facility);
        return $this->_isOpen;
    }

    public function execute($priority, $message) {
        if (!empty($message)) {
            if ($this->_isOpen) {
                if (is_array($message)) {
                    foreach ($message as $msg) {
                        syslog($priority, $message);
                    }
                } else {
                    syslog($priority, $message);
                }
                return TRUE;
            }
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
