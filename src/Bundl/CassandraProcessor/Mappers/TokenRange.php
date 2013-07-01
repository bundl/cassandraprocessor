<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor\Mappers;

use Bundl\CassandraProcessor\Events;
use Cubex\Events\EventManager;
use Cubex\Mapper\Database\RecordMapper;

/**
 * Class TokenRange
 * @package Bundl\CassandraProcessor\Mappers
 *
 * @index processing,hostname
 * @index processing,processed
 * @index randomKey
 * @unique startToken
 * @engine InnoDB
 */
class TokenRange extends RecordMapper
{
  /**
   * @datatype varchar(128)
   * @notnull
   */
  public $startToken = '';
  /**
   * @datatype varchar(128)
   * @notnull
   */
  public $endToken = '';
  /**
   * @datatype varchar(255)
   * @notnull
   */
  public $firstKey = '';
  /**
   * @datatype varchar(255)
   * @notnull
   */
  public $lastKey = '';
  /**
   * @datatype bool
   * @notnull
   */
  public $processing = 0;
  /**
   * @datatype bool
   * @notnull
   */
  public $processed = 0;
  /**
   * @datatype bool
   * @notnull
   */
  public $failed = 0;
  /**
   * @datatype bigint
   * @unsigned
   * @notnull
   */
  public $processingTime = 0;
  /**
   * @datatype bigint
   * @unsigned
   * @notnull
   */
  public $totalItems = 0;
  /**
   * @datatype bigint
   * @unsigned
   * @notnull
   */
  public $processedItems = 0;
  /**
   * @datatype bigint
   * @unsigned
   * @notnull
   */
  public $errorCount = 0;
  /**
   * @datatype int
   * @unsigned
   * @notnull
   */
  public $randomKey = 0;
  /**
   * @datatype varchar(1024)
   */
  public $error;
  /**
   * @datatype varchar(255)
   */
  public $hostname;
  /**
   * @datatype int
   * @unsigned
   * @notnull
   */
  public $requeueCount = 0;

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
