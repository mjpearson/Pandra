<?php

interface PandraLogger {

    public function __construct(array $params);

    public function isOpen();

    public function execute($priority, $message);

}
?>
