<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Cubex\Log\Log;

class BatchSizeTuner
{
  private $_currentBatchSize;
  private $_bufferSize;
  private $_lastBatchTime;
  private $_batchTimes;
  private $_started;
  private $_fixedSize;

  private $_minBatchSize = 25;
  private $_maxBatchSize = 250;
  private $_minBatchTime = 5;
  private $_maxBatchTime = 20;


  /**
   * @param int $initialBatchSize The size of the batches to start with
   * @param int $bufferSize       The number of batches to average the time
   *                              over before deciding to change the batch size
   */
  public function __construct($initialBatchSize = 50, $bufferSize = 10)
  {
    $this->_bufferSize       = $bufferSize;
    $this->_currentBatchSize = $initialBatchSize;
    $this->_fixedSize = false;
    $this->reset();
  }

  public function setBatchSizeLimitsArr($sizeArr)
  {
    if(is_integer($sizeArr))
    {
      $this->setBatchSizeLimits($sizeArr, $sizeArr);
    }
    else
    {
      $this->setBatchSizeLimits($sizeArr['min'], $sizeArr['max']);
    }
  }

  public function setBatchSizeLimits($minSize, $maxSize)
  {
    $this->_minBatchSize = $minSize;
    $this->_maxBatchSize = $maxSize;

    $this->_fixedSize = $minSize == $maxSize;
    if($this->_fixedSize)
    {
      $this->_currentBatchSize = $minSize;
      Log::info('BatchSizeTuner: Batch size fixed at ' . $minSize);
    }
    else
    {
      Log::info('BatchSizeTuner: Batch size range set to min=' . $minSize . ', max=' . $maxSize);
    }
  }

  public function setBatchTimeLimits($minTime, $maxTime)
  {
    $this->_minBatchTime = $minTime;
    $this->_maxBatchTime = $maxTime;
  }

  public function reset()
  {
    $this->_batchTimes = [];
    $this->_started    = false;
  }

  public function nextBatch()
  {
    if($this->_fixedSize)
    {
      return;
    }

    $now = microtime(true);
    if($this->_started)
    {
      $batchDuration       = $now - $this->_lastBatchTime;
      $this->_batchTimes[] = $batchDuration;
      $this->_recalculateBatchSize();
    }
    else
    {
      $this->_started       = true;
    }
    $this->_lastBatchTime = $now;
  }

  public function getBatchSize()
  {
    return $this->_currentBatchSize;
  }

  private function _recalculateBatchSize()
  {
    if((count($this->_batchTimes) < $this->_bufferSize) || $this->_fixedSize)
    {
      return;
    }

    while(count($this->_batchTimes) > $this->_bufferSize)
    {
      array_shift($this->_batchTimes);
    }

    $avgTime = array_sum($this->_batchTimes) / count($this->_batchTimes);

    $newBatchSize = $this->_currentBatchSize;
    if($avgTime < $this->_minBatchTime)
    {
      // Batches are processing too fast. Increase the batch size if possible.
      $diff          = $this->_minBatchTime - $avgTime;
      $proportion    = $diff / $avgTime;
      $batchIncrease = $this->_currentBatchSize * $proportion;
      $newBatchSize  = round($this->_currentBatchSize + $batchIncrease);
      if($newBatchSize > $this->_maxBatchSize)
      {
        $newBatchSize = $this->_maxBatchSize;
      }
    }
    else if($avgTime > $this->_maxBatchTime)
    {
      // Batches are processing too slowly. Reduce the batch size if possible.
      $diff          = $avgTime - $this->_maxBatchTime;
      $proportion    = $diff / $avgTime;
      $batchDecrease = $this->_currentBatchSize * $proportion;
      $newBatchSize  = round($this->_currentBatchSize - $batchDecrease);
      if($newBatchSize < $this->_minBatchSize)
      {
        $newBatchSize = $this->_minBatchSize;
      }
    }

    if($newBatchSize != $this->_currentBatchSize)
    {
      Log::info(
        sprintf(
          'BatchSizeTuner: Average batch time: %d seconds. Changing batch size from %d to %d',
          $avgTime,
          $this->_currentBatchSize,
          $newBatchSize
        )
      );
      $this->_currentBatchSize = $newBatchSize;
      $this->reset();
    }
    else
    {
      Log::debug(
        'BatchSizeTuner: Average batch time: ' . $avgTime . ' seconds, batch size=' . $this->_currentBatchSize
      );
    }
  }
}
