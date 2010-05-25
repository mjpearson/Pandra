<?php
/**
 * PandraLoggerMail
 *
 * Mail implementation of PandraLogger.  Handles all log messages crit to emergency
 *
 * @author Michael Pearson <pandra-support@phpgrease.net>
 * @copyright 2010 phpgrease.net
 * @license http://www.gnu.org/licenses/lgpl.html GNU Lesser General Public License
 * @version 0.2
 * @package pandra
 */
class PandraLoggerMail implements PandraLogger {

    private $_mailFrom = '';

    private $_mailTo = '';

    private $_subject = '';

    private $_maxPriority = PandraLog::LOG_CRIT;

    private $_sendDelay = FALSE;

    private $_headers = '';

    private $_messages = '';

    /**
     *
     * @param array $params 'from', 'to', 'subject', 'delay' (sends on destruct)
     */
    public function __construct(array $params) {
        if (isset($params['from'])) $this->_mailFrom = $params['from'];
        if (isset($params['to'])) $this->_mailFrom = $params['to'];
        if (isset($params['subject'])) $this->_mailFrom = $params['subject'];
        if (isset($params['delay'])) $this->_sendDelay = $params['delay'];
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
            $this->_message .= $message;
            if (!$this->_sendDelay) {
                return $this->sendMail();
            }
            return TRUE;
        }
        return FALSE;
    }

    public function sendMail() {
        if (!empty($this->_messages)) {
            $this->subject = '['.strtoupper(PandraLog::$priorityStr[$priority]).'] '.$this->_subject;

            if (!empty($this->_mailFrom)) {
                $this->_headers = "From: ".$this->_mailFrom."\r\n" .
                        "Reply-To: ".$this->_mailFrom."\r\n";
            }
            $this->_headers .= "X-Mailer: PHP/".phpversion();
            return mail($this->_mailTo, $this->_subject, $this->messages, $this->_headers);
        }
        return FALSE;
    }

    public function close() {
        if ($this->_sendDelay) {
            $this->sendMail();
        }
    }

    public function  __destruct() {
        $this->close();
    }
}
?>
