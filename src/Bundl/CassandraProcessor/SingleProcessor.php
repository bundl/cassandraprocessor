<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class SingleProcessor extends ItemProcessor
{
  public function supportsBatchProcessing()
  {
    return false;
  }

  public function processBatch(array $items)
  {
    throw new \Exception('Batch processing not supported');
  }
}
