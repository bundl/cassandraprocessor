<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor\Mappers;

use Bundl\CassandraProcessor\Events;
use Cubex\Events\EventManager;
use Cubex\Mapper\Database\RecordMapper;

class TokenRange extends RecordMapper
{
  public $startToken;
  public $endToken;
  /**
   * @datatype string
   */
  public $firstKey;
  /**
   * @datatype string
   */
  public $lastKey;
  /**
   * @datatype int
   */
  public $processing = 0;
  /**
   * @datatype int
   */
  public $processed = 0;
  /**
   * @datatype int
   */
  public $failed = 0;
  /**
   * @datatype int
   */
  public $processingTime = 0;
  /**
   * @datatype int
   */
  public $totalItems = 0;
  /**
   * @datatype int
   */
  public $processedItems = 0;
  /**
   * @datatype int
   */
  public $errorCount = 0;
  /**
   * @datatype int
   */
  public $randomKey = 0;
  public $error;
  public $hostname;

  protected $_schemaType = self::SCHEMA_CAMELCASE;

  private static $_tokenRangesTableName = 'token_ranges';

  public static function setTableName($tableName)
  {
    self::$_tokenRangesTableName = $tableName;
  }

  public function getTableName()
  {
    return self::$_tokenRangesTableName;
  }

  public function saveChanges()
  {
    EventManager::trigger(Events::RANGE_SAVE_CHANGES_START);
    parent::saveChanges();
    EventManager::trigger(Events::RANGE_SAVE_CHANGES_END);
  }

  public function createdAttribute()
  {
    return 'createdAt';
  }
  public function updatedAttribute()
  {
    return 'updatedAt';
  }
}
