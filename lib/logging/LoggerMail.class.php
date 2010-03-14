<?php

class PandraLoggerMail implements PandraLogger {

    private $_mailFrom = '';

    private $_mailTo = '';

    private $_subject = '';

    private $_maxPriority = PandraLog::LOG_CRIT;

    public function __construct(array $params) {
        if (isset($params['from'])) $this->_mailFrom = $params['from'];
        if (isset($params['to'])) $this->_mailFrom = $params['to'];
        if (isset($params['subject'])) $this->_mailFrom = $params['subject'];
    }

    public function isOpen() {
        return function_exists('mail');
    }

    /**
     * Mail shouldn't handle notices, info's etc.
     * @param int $priority requested priority log
     * @return boolean this logger will log for priority
     */
    public function isPriorityLogger($priority) {
        return ($priority <= $this->_maxPriority);
    }

    /**
     * mailFrom mutator
     * @param string $mailFrom set the email from address
     */
    public function setMailFrom($mailFrom) {
        $this->_mailFrom = $mailFrom;
    }

    /**
     * mailFrom accessor
     * @return string email from address
     */
    public function getMailFrom() {
        return $this->_mailFrom;
    }

    /**
     * mailTo mutator
     * @param string $mailTo set the email to address
     */
    public function setMailTo($mailTo) {
        $this->_mailTo = $mailTo;
    }

    /**
     * mailTo accessor
     * @return string email to address
     */
    public function getMailTo() {
        return $this->_mailTo;
    }


    /**
     * subject mutator
     * @param string $subject set the email subject line
     */
    public function setSubject($subject) {
        $this->_subject = $subject;
    }

    /**
     * subject accessor
     * @return string email subject line
     */
    public function getSubject() {
        return $this->_subject;
    }


    public function execute($priority, $message) {
        if ($this->isPriorityLogger($priority) && $this->_isOpen) {
            $subject = '['.strtoupper(PandraLog::$priorityStr[$priority]).'] '.$this->_subject;
            $headers = '';
            if (!empty($this->_mailFrom)) {
                $headers = "From: ".$this->_mailFrom."\r\n" .
                            "Reply-To: ".$this->_mailFrom."\r\n";
            }
            $headers .= "X-Mailer: PHP/".phpversion();

            return mail($this->_mailTo, $subject, $message, $headers);
        }
        return FALSE;
    }
}
?>