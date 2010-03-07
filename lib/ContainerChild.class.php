<?php
/**
 * (c) 2010 phpgrease.net
 *
 * For licensing terms, plese see license.txt which should distribute with this source
 *
 * Container Binding interface dictates expected entry points into underlying columns,
 * to maintain internal relational integrity
 *
 * @package Pandra
 * @author Michael Pearson <pandra-support@phpgrease.net>
 */

/**
 * @abstract
 */
interface PandraContainerChild {

    /**
     * Column object function to call column unset in the parent (parent->unset($this->getName()) for instance)
     */
    public function detach();

    /**
     * Sets parent to null
     * @param bool $localUnbind optional call local detach() method prior to nullifying
     */
    public function nullParent($localUnbind = TRUE);
    
    /**
     * Sets parent
     * @param object $parent Container object
     * @param bool $bindToParent optional make parent aware of new column via addColumnObj
     */
    public function setParent($parent, $bindToParent = TRUE);

    /**
     * Parent accessor
     * @return Parent container
     */
    public function getParent();

    /**
     * Local reset method, handling delete/modified flags
     */
    public function reset();

    /**
     * Delete method
     */
    public function delete();

    /**
     * Modified accessor
     * @return bool child is marked as modified
     */
    public function isModified();

    /**
     * Deleted accessor
     * @return bool child is marked for deletion
     */
    public function isDeleted();

    /**
     * Child name accessor
     * @return string child name
     */
    public function getName();




}
?>
