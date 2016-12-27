<?php

/**
 * @file
 * Contains \Drupal\query_cache\KVCUDQuery.
 */

namespace Drupal\query_cache;

require_once __DIR__ . '/CUDQuery.php';

/**
 * A class for invalidating KV queries on CUD operations.
 */
class KVCUDQuery extends CUDQuery {

  /**
   * {@inheritdoc}
   */
  public function invalidateCache() {
    $columns = $this->config['primary_key'];
    $key = $this->getKVCacheKey();

    if (empty($columns) || $key === FALSE) {
      $this->clearAll();
      return;
    }

    $this->clear($key);
    $this->clear($this->queryCacheKeyPrefix() . ':', TRUE);
  }

}
