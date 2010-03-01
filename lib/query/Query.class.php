<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * @package Pandra
 * @link http://www.phpgrease.net/projects/pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */

class PandraQuery implements ArrayAccess {

    //
    const POPULATE_ARRAY = 1;

    const POPULATE_JSON = 2;

    const POPULATE_SCALAR = 3;

    //
    const CONTEXT_COLUMN = 1;

    const CONTEXT_SUPERCOLUMNFAMILY = 2;

    const CONTEXT_COLUMNFAMILY = 3;

    private $_context = NULL;

    private $_graph = array();

    private $_graphKeys = array('keySpace', 'column_family', 'column', 'super_column');

    public $_columns = array();

    private $_width = 0;

    public function init() {

    }

    // @todo move to callStatic with PHP5.3 upgrade
    // anonymous claus calls
    public function __call($class, $args) {
        if (strpos($class, 'Clause') === 0) {
            $class = 'Pandra'.$class;
            if (class_exists($class)) {
                return new $class(array_pop($args));
            }
        }
    }

    /**
     * Query stub (predicate search/slice range etc)
     */
    public function extract() {
        return $this->_columns;
    }

    public function setColumn($columnName, $value) {
        $this->_columns[$columnName] = $value;

    }

    /**
     * Sets value with a validator.  To skip validation, use explicit
     * PandraColumn->setValue($value, FALSE); or do not provide a typeDef
     * @param string $offset column name
     * @param mixed $value new column value
     */
    public function offsetSet($offset, $value) {
//        $this->__set($offset, $value);
    }

    /**
     * Check for column existence
     * @param string $offset column name
     * @return bool column eixsts
     */
    public function offsetExists($offset) {
//        return isset($this->_columns[$offset]);
    }

    /*
     * This only unsets the column in the container, do delete use the
     * PandraColumn->delete() function
     * @return void
    */
    public function offsetUnset($offset) {
//        unset($this->_columns[$offset]);
    }

    /**
     * Get column value by column name
     * @param string $offset column name
     * @return mixed column value
     */
    public function offsetGet($match) {
        switch ($this->_width) {
            // Keyspace
            case 0 :
            // ColumnFamily
            case 1 :
                if (is_string($match)) {
                    $this->_graph[$this->_graphKeys[$this->_width]] = $match;
                } else {
                    throw new RuntimeException('ColumnFamily must be a string');
                }
                break;
            // Columns
            case 2 :
                $this->_graph[$this->_graphKeys[$this->_width]] = $match;
                //$this->_graph['columns'] = $match;
                break;
            //  Columns in SuperColumn
            case 3 :
                $this->_graph[$this->_graphKeys[$this->_width + 1]] = $this->_graph[$this->_graphKeys[$this->_width]];
                $this->_graph[$this->_graphKeys[$this->_width]] = $match;
                break;
            default:
                new RuntimeException('Query exceeds maximum width');
        }
        $this->_width++;
        return $this;
    }

    public function reset() {
        $this->_width = 0;
    }

    private function isClause($match) {
        return $match instanceof PandraClause;
    }

    public function load($keys, $populator = self::POPULATE_ARRAY) {
        // stub
        return;

        $columnPath = new cassandra_ColumnPath();
//         array(
//                        'column_family' => $columnFamilyName,
//                        'super_column' => $superColumnName,
//                        'column' => $columnName
//        ));




        $literal = TRUE;

        foreach ($this->_graph as $path => $match) {


            if ($this->isClause($match)) {
                $literal = FALSE;
            } else {
                if ($path == 'columnFamily') {
                    $columnPath->column_family = $match;
                } elseif ($path == '') {

                }
            }
        }


        var_dump($this);
        // lets see what we can get from the graph




    }

}
?>