<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

class Events
{
  const REFRESH_KEYS_START       = 'cassandraprocessor.refreshKeys.start';
  const REFRESH_KEYS_END         = 'cassandraprocessor.refreshKeys.end';
  const CASS_CONNECT_START       = 'cassandraprocessor.cassConnect.start';
  const CASS_CONNECT_END         = 'cassandraprocessor.cassConnect.end';
  const GET_KEYS_START           = 'cassandraprocessor.getKeys.start';
  const GET_KEYS_END             = 'cassandraprocessor.getKeys.end';
  const CLAIM_RANGE_START        = 'cassandraprocessor.claimRange.start';
  const CLAIM_RANGE_END          = 'cassandraprocessor.claimRange.end';
  const REQUEUE_RANGE_START      = 'cassandraprocessor.requeueRange.start';
  const REQUEUE_RANGE_END        = 'cassandraprocessor.requeueRange.end';
  const RANGE_SAVE_CHANGES_START = 'cassandraprocessor.rangeSaveChanges.start';
  const RANGE_SAVE_CHANGES_END   = 'cassandraprocessor.rangeSaveChanges.end';
}
