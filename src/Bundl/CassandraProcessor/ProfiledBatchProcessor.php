<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class ProfiledBatchProcessor extends BatchProcessor
{
  protected $_profiler;

  public function __construct()
  {
    $this->_profiler = new Profiler();
  }
}
