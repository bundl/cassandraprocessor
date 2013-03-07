<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor\Mappers;

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

  protected $_autoTimestamp = false;
  protected $_schemaType = self::SCHEMA_CAMELCASE;

  public function getTableName()
  {
    return 'token_ranges';
  }
}
