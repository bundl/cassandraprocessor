<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class SingleProcessor implements ItemProcessor
{
  public function supportsBatchProcessing()
  {
    return false;
  }

  public function processBatch($items)
  {
    throw new \Exception('Batch processing not supported');
  }
}
