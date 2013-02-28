<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class BatchProcessor implements ItemProcessor
{
  public function supportsBatchProcessing()
  {
    return true;
  }

  public function processItem($key, $itemData)
  {
    throw new Exception('Only supports batch processing');
  }
}
