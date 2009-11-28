<?
class validate {
	
	static function test($fieldName, &$fieldValue, $funcs, &$messages) {
		$validators = explode(",", $funcs);

		$ok = FALSE;

		foreach ($validators as $validator) {
           
			// check for basic validator types
			switch ($validator) {
		case 'honeypot' :
			$ok = empty($fieldValue);
			return $ok;
                case 'notempty' :
                    $ok = !empty($fieldValue);
                    if (!$ok) {                
                        $messages[] = "$fieldName : Required Field<br>";
                    }
                    $emptyField = !$ok;
                    break;
				case 'email' :
					$ok = filter_var($fieldValue, FILTER_VALIDATE_EMAIL);
					if (!$ok) $messages[] = "$fieldName : Invalid email address<br>";
					break;
				case 'url' :
					$ok = filter_var($fieldValue, FILTER_VALIDATE_URL);
					if (!$ok) $messages[] = "$fieldName : Invalid URL<br>";
					break;
				case 'int' :
                    $ok = is_numeric($fieldValue);
                    if ($ok) {
                        $ok = ((int) $fieldValue) == $fieldValue;
                    }
                    break;
				case 'float' :
                    $ok = is_numeric($fieldValue);
                    if ($ok) {
                        $ok = ((float) $fieldValue) == $fieldValue;
                    }
                    break;
				case 'numeric' :
                    $ok = is_numeric($fieldValue);
                    break;
				case 'string' :
                    $ok = is_string($fieldValue);					
					break;
				default :
					break;
			}
            if ($emptyField) {
                break;
            } else {
//                if (!$ok) $messages[] = "$fieldName : Field error, expecting $validator<br>";
            }
		}        
		return $ok;
	}
}
?>
