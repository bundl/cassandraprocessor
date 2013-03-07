<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class BatchProcessor extends ItemProcessor
{
  public function supportsBatchProcessing()
  {
    return true;
  }

  /**
   * @param string $key
   * @param array  $itemData
   *
   * @return bool
   * @throws \Exception
   */
  public function processItem($key, $itemData)
  {
    throw new \Exception('Only supports batch processing');
  }
}
