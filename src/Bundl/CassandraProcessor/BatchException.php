<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

class BatchException extends \Exception
{
  private $_errorCount;
  private $_errorKeys;

  /**
   * @param int $errorCount
   * @param string $message
   * @param array  $errorKeys
   */
  public function __construct($errorCount, $message = "", $errorKeys = array())
  {
    parent::__construct($message);
    $this->_errorCount = $errorCount;
    $this->_errorKeys = $errorKeys;
  }

  /**
   * @return int
   */
  public function getErrorCount()
  {
    return $this->_errorCount;
  }

  public function getErrorKeys()
  {
    return $this->_errorKeys;
  }
}
