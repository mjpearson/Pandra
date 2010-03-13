<?php

interface PandraLogger {

    public function __construct(array $params);

    public function isOpen();

    public function isPriorityLogger($priority);

    public function execute($priority, $message);

}
?>
