<?php
class PandraLoggerFirePHP implements PandraLogger {

    private $_isOpen = FALSE;

    private $_fb = NULL;

    public function __construct(array $params) {
        $this->_isOpen = class_exists('FB');
        if ($this->_isOpen) {
            $this->_fb = FirePHP::getInstance(true);
        }
        return $this->_isOpen;
    }

    public function isOpen() {
        return $this->_isOpen;
    }

    public function execute($priority, $message) {
        if ($this->_isOpen) {
            switch ($priority) {
                case PandraLog::LOG_NOTICE :
                    $this->_fb->log($message);
                    break;
                case PandraLog::LOG_INFO :
                    $this->_fb->info($message);
                    break;
                case PandraLog::LOG_WARNING :
                    $this->_fb->warn($message);
                    break;
                default:
                    $this->_fb->error($message);
            }
        }
        return FALSE;
    }
}
?>
