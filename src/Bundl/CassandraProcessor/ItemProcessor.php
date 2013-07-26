<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class ItemProcessor
{
  /**
   * @var \Cubex\Cassandra\ColumnFamily
   */
  public $sourceColumnFamily = null;

  /**
   * @return bool
   */
  public abstract function supportsBatchProcessing();

  /**
   * @param array $items
   * @return int The number of items that were processed excluding any that were skipped
   *
   * @throws ItemException|\Exception
   */
  public abstract function processBatch(array $items);

  /**
   * @param string $key
   * @param array  $itemData
   * @return bool true if the item was processed, false if it was skipped
   *
   * @throws ItemException|\Exception
   */
  public abstract function processItem($key, $itemData);

  /**
   * List of columns required from the Cassandra items.
   * null = all columns, array() = none (just keys)
   *
   * @return null|array
   */
  public function requiredColumns()
  {
    return null;
  }

  /**
   * Return true to stop on all errors
   *
   * @return bool
   */
  public function stopOnErrors()
  {
    return false;
  }

  /**
   * Return true to save progress after each batch
   *
   * @return bool
   */
  public function shouldSaveProgress()
  {
    return false;
  }

  /**
   * Return the min/max size of the batches to process
   *
   * @return array An array containing 'min' and 'max' values
   */
  public function getBatchSize()
  {
    return array('min' => 50, 'max' => 250);
  }
}
