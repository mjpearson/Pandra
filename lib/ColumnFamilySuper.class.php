<?php

abstract class PandraColumnFamilySuper extends PandraColumnFamily {

    /* @var int column family type (standard or super) */
    private $_type = PANDRA_CF_SUPER;

    public function addSuper(PandraSuperColumn $scObj) {
        $superName = $scObj->name;

        $scObj->setParentCF($this); 
        $this->columns[$superName] = $scObj;

        return $this->getColumn($superName);
    }

    
}