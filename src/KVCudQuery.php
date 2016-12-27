<?php

namespace QueryCache;

/**
 * A class for invalidating key-value queries on CUD operations.
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
