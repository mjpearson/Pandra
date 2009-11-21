<?php

class PandraColumn {
	
	public $typeDef = array();

	public $lastError = array();

	public $callback = NULL;

	public $modified = FALSE;

	public $name = NULL;

	public $value = NULL;

        public function setValue($value, $validate = TRUE) {
            if ($validate && !empty($this->typeDef)) {
                if (!PandraValidator::check($value, $this->name, $this->typeDef, $this->lastError)) {
                    return FALSE;
                }
            }

            $this->value = $value;
            $this->modified = TRUE;
            return TRUE;
        }

        public function callbackValue() {
            $value = '';
            eval('$value = '.$cObj->callback.'("'.$cObj->value.'");');
            return $value;
        }
}
