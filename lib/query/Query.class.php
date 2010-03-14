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

class PandraQuery {

    //
    const POPULATE_ARRAY = 1;

    const POPULATE_JSON = 2;

    const POPULATE_SCALAR = 3;

    const POPULATE_MODEL = 3;

    const CONTEXT_COLUMN = 0;

    const CONTEXT_SUPERCOLUMN = 1;

    const CACHE_NONE = 0;

    const CACHE_APC = 1;

    const CACHE_MEM = 2;

    private $_graph = array();

    private $_keySpace = NULL;

    private $_columnFamily = NULL;

    private $_keys = array();

    private $_graphTarget = NULL;

    private $_limit = 0;

    private $_cacheScheme = self::CACHE_NONE;

    private $_cacheExpirySeconds = 60;

    public function select($columnFamilyName) {
        list($keySpace, $columnFamily) = explode('.', $columnFamilyName);
        if (empty($keySpace) || empty($columnFamily)) {
            $message = 'Keyspace or ColumnFamily missing.  Expect "Keyspace.ColumnFamilyName"';
            PandraLog::crit($message);
            throw new RuntimeException($message);
        }

        $this->_keySpace = $keySpace;
        $this->_columnFamily = $columnFamily;

        return $this;
    }

    // @todo move to callStatic with PHP5.3 upgrade
    // anonymous claus calls
    public function __call($class, $args) {
        $class = 'PandraClause'.$class;
        if (class_exists($class)) {
            return new $class(array_pop($args));
        } else {
            return NULL;
        }
    }

    public function whereKeyIn(array $keys) {
        $this->_keys = $keys;
        return $this;
    }

    private function graphContext($context, PandraClause $match) {
        $idx = isset($this->_graph[$context]) ? count($this->_graph[$context]) : 0;
        $this->_graph[$context][$idx] = array(
                'target' => $match,
                'filter' => array()
        );
        $this->_context = $context;
        $this->_graphTarget = &$this->_graph[$context][$idx];
    }

    public function whereColumn(PandraClause $match) {
        $this->graphContext(self::CONTEXT_COLUMN, $match);
        return $this;
    }

    public function whereValue(PandraClause $match) {
        // whereValue is only against colum types
        if ( $this->_context == self::CONTEXT_COLUMN &&
                is_array($this->_graph[self::CONTEXT_COLUMN]) ) {
            $this->_graphTarget['filter'][] = $match;
        }
        return $this;
    }

    public function whereSuperColumn(PandraClause $match) {
        $this->graphContext(self::CONTEXT_COLUMN, $match);
        return $this;
    }

    public function limit($limit) {
        if (is_numeric($limit)) {
            $this->_limit = $limit;
        }

        return $this;
    }

    public function pathOK() {
        return (!empty($this->_keys) &&
                        !empty($this->_keySpace) &&
                        !empty($this->_columnFamily) &&
                        count($this->_graph));
    }

    public function count() {
        return $this->extract(self::POPULATE_SCALAR);
    }

    public function extract($populate = self::POPULATE_MODEL) {

        // Check we have everything
        if ($this->pathOK()) {

            $result = NULL;

            // Cache hit?
            if ($this->cacheRetrieve($result)) {
                return $this->hydrate($populate, $result);
            }

            // Graph Processing
            PandraLog::debug('processing graph');

            if (!empty($this->_graph[self::CONTEXT_SUPERCOLUMN])) {

            }

            $preResult = PandraCore::getCFSliceMulti($this->_keySpace, $this->_keys, $this->_columnFamily);
                //public function getCFSliceMulti($keySpace, array $keyIDs, $columnFamilyName, $superColumnName = NULL, $columnNames = array(), $consistencyLevel = NULL) {

            // We've got a basic result set, so filter out what we don't want


            // Cache and populate
            if ($result !== NULL) {
                $this->cacheStore($result);
                return $this->hydrate($populate, $result);
            }
        } else {
            throw new RuntimeException('Missing Keys, Keyspace or ColumnFamily');
        }

        return NULL;
    }


    private function cacheRetrieve(&$result) {
        if ($this->_cacheScheme !== self::CACHE_NONE) {
            $cacheKey = $this->cacheKey();
            switch($this->_cacheScheme) {
                case self::CACHE_APC :
                    $status = FALSE;
                    $result = apc_fetch($cacheKey, $status);
                    if (!$result) $result = NULL;
                    return $status && ($result !== NULL);

                    // Otherwise it's a cache miss, continue
                    break;

                case self::CACHE_MEM :
                // @todo
                    break;

                default: break;
            }
        }
        return FALSE;
    }

    private function cacheStore($result) {
        // Cache result
        if ($this->_cacheScheme !== self::CACHE_NONE) {
            $cacheKey = $this->cacheKey();
            switch($this->_cacheScheme) {
                case self::CACHE_APC :
                    return apc_store($cacheKey, $result, $this->_cacheExpirySeconds);
                    break;

                case self::CACHE_MEM :
                // @todo
                    break;

                default: break;
            }
        }

        return FALSE;
    }


    public function apc() {
        if (!PandraCore::getAPCAvailable()) {
            PandraLog::warn('APC Unavailable');
        } else {
            $this->_cacheScheme = self::CACHE_APC;
        }
        return $this;
    }

    public function memcached() {
        if (!PandraCore::getMemcachedvailable()) {
            PandraLog::warn('Memcached Unavailable');
        } else {
            $this->_cacheScheme = self::CACHE_MEM;
        }
        return $this;
    }

    public function nocache() {
        $this->_cacheSchema = self::CACHE_NONE;
    }

    private function cacheKey() {
        // NASTY!
        return md5($this->_keySpace.
                $this->_columnFamily.
                serialize($this->_keys).
                serialize($this->_graph).
                $this->_limit);
    }
}
?>