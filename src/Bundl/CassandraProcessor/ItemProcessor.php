<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

abstract class ItemProcessor
{
  /**
   * @var \Cubex\KvStore\Cassandra\ColumnFamily
   */
  public $sourceColumnFamily;

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
  public abstract function processBatch($items);

  /**
   * @param string $key
   * @param array  $itemData
   * @return bool true if the item was processed, false if it was skipped
   *
   * @throws ItemException|\Exception
   */
  public abstract function processItem($key, $itemData);

  /**
   * List of columns required from the Cassandra items. null = all columns, array() = none (just keys)
   *
   * @return null|array
   */
  public abstract function requiredColumns();

  /**
   * Return true to stop on all errors
   *
   * @return bool
   */
  public abstract function stopOnErrors();

  /**
   * Return true to save progress after each batch
   *
   * @return bool
   */
  public abstract function shouldSaveProgress();
}
