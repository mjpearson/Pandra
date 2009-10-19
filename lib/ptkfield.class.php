<?
/**
 * PHPGrease::DBOField field abstraction, validator and encrypter
 * @link {mailto:slig.h@x0rz.com}
 */
class ptkfield {

	/* @var string save value of this field (will populate to database) */
	private $value = '';

	/* @var string encryption callback function for TYPE_CRYPT field */
	private $cryptCBFunc = NULL;

	/* @var string validator plugin for this field */
	private $valCBFunc = NULL;

	/* @var int field is of special TYPE_ */
    	private $type = NULL;

	/* @var bool this field is a primary key */
	var $isPKey = FALSE;

	/* @var string last validation error */
	private $validationError = '';

	/* @var bool validation default for dbofields */
	public $validateEnabled = self::VALIDATE;

	// special column types
	const TYPE_NONE = 0;	// none
	const TYPE_PKEY = 1;	// primary key
	const TYPE_IDX = 2;	// index type
	const TYPE_CRYPT = 3;	// Crypt on store via cryptCBFunc
	const TYPE_CRYPT_SOFT = 4;	// Crypt at dbo insert/update level

	// validation flag constants
	const VALIDATE = TRUE;
	const NO_VALIDATE = FALSE;

	/**
	 * field constructor
	 * @param string $valueDefault default value of this field
	 * @param string $valCBFunc validation callback function
	 * @param int $type special column type
	 * @param string $cryptCBFunc encrypt callback type for TYPE_CRYPT field
	 */
	public function __construct($valueDefault = NULL, $valCBFunc = NULL, $cryptCBFunc = 'md5') {

		// set validation callback
		if (!empty($valCBFunc)) {			
			$this->valCBFunc = $valCBFunc;
		}

		// set value
		$this->setValue($valueDefault);

		// setup types
		$this->isPKey = ($type == self::TYPE_PKEY);
		
		// handle encrypted storage data types
		if ($type == self::TYPE_CRYPT) {
			$this->cryptCBFunc = $cryptCBFunc;
			$this->cryptValue = eval($cryptCallback."(".$this->value.")");
		}
	}

	/**
	 * Runs encrypt callback function (eval wrapper)
	 */
	private function cryptCallback() {
		$this->value = eval($this->cryptCBFunc."(".$this->value.")");
	}

	/**
	 * Validates a field
	 * @param string $errorMsg custom error message for field validation error
	 * @return bool field validated correctly
	 */
	public function validate($errorMsg = "") {
		// fields without a validator will always return true
		if (empty($this->valCBFunc)) return TRUE;

		// find multiple validators
		$validators = explode("/|/", $this->valCBFunc);

		$error = FALSE;

		foreach ($validators as $validator) {
			// check for basic validator types
			switch ($this->valCBFunc) {
				// PHP 'Filter' functions
				case 'email' :
					$error = !filter_var($this->value, FILTER_VALIDATE_EMAIL);
					if ($error && empty($errorMsg)) $errorMsg = "Invalid email address ".$this->valCBFunc;
					break;
				case 'url' :
					$error = !filter_var($this->value, FILTER_VALIDATE_URL);
					if ($error && empty($errorMsg)) $errorMsg = "Invalid URL ".$this->valCBFunc;
					break;
				case 'int' :
				case 'double' :
				case 'float' :
				case 'long' :
				case 'real' :
				case 'numeric' :
				case 'string' :
					$error = !(eval('is_'.$this->valCBFunc.'('.$this->value.')'));
					if ($error && empty($errorMsg)) $errorMsg = "Field error, expected ".$this->valCBFunc;
					break;
				case 'strlen' :
					break;
				default :
					// check that the callback class can be found...
					$vFileName = dirname(__FILE__).'/plugins/validators/'.$this->valCBFunc.'.class.php';
					if (!file_exists($vFileName)) {
						$error = TRUE;
						$errorMsg = "Validate class '".$this->valCBFunc."' not found in ".$vFileName;
					} else {
						// @todovalidator plugins
						//require_once($vFileName);
						//$error = $valCBFunc::isValid($errorMsg);
					}
					break;
			}
		}

		if ($error) {
			$this->validationError = $errorMsg;
		}
		return !$error;
	}

	/**
	 * get last error
	 * @return string last validation error
	 */
	public function lastError() {
		return $this->validationError;
		
	}

	/**
	 * Get the 'value' of this abstraction field.
	 * @return mixed value of this dboField.
	 */
	public function getValue() {
		return $this->value;
	}

	/**
	 * Get column type for this field
	 * @return int TYPE_ const
	 */
	public function getType() {
		return $this->type;
	}

	/**
	 * Attempt to set value for this field, including validation and encryption
	 * @param mixed $value value to set for this dboField
	 * @return bool set was successful
	 */
	public function setValue($value) {
		if ($this->validateEnable && !$this->validate($value)) {
			return FALSE;
		}
		
		$this->value = $value;
		
		if ($this->type == self::TYPE_CRYPT) {
			$this->cryptCallback();
		}
	}
}

/*
 * Base validation class. Individual plugins should extend
 *
 */
interface DBOFieldValidator {

	/*
	 * Given a value, determine its type validity for the dboField
	 * @param string $value value to validate
	 * @return bool field validated ok
	 */
	public function isValid($value, $errorMsg = '');
}
?>
