<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

class ItemException extends \Exception
{
  private $_itemKey;

  public function __construct($itemKey, $message = "")
  {
    parent::__construct($message);
    $this->_itemKey = $itemKey;
  }

  public function getItemKey()
  {
    return $this->_itemKey;
  }
}
