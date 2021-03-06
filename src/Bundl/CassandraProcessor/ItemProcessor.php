<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use cassandra\SlicePredicate;
use cassandra\SliceRange;

abstract class ItemProcessor
{
  /**
   * @var \Cubex\Cassandra\ColumnFamily
   */
  public $sourceColumnFamily = null;

  /**
   * @var mixed
   */
  protected $_rangeData = null;

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
   * If an integer is returned then the first n columns will be fetched
   *
   * @return null|array|int|SlicePredicate|SliceRange
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

  /**
   * Reset the current range data at the start of a new range
   */
  public function resetRangeData()
  {
    $this->_rangeData = null;
  }

  /**
   * Get the current range data
   *
   * @return mixed
   */
  public function getRangeData()
  {
    return $this->_rangeData;
  }

  /**
   * Set the current range data
   *
   * @param mixed $data
   */
  public function setRangeData($data)
  {
    $this->_rangeData = $data;
  }
}
