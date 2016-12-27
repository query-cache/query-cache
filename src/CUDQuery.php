<?php

/**
 * @file
 * Contains \Drupal\query_cache\CUDQuery.
 */

namespace Drupal\query_cache;

require_once __DIR__ . '/Query.php';

/**
 * A class for invalidating queries on CUD operations.
 */
class CUDQuery extends Query {

  public function execute() {
    $this->invalidateCache();
    return $this->executeOriginalQuery();
  }

  public function invalidateCache() {
    $this->clearAll();
  }

  protected function clearAll() {
    $this->clear('*', TRUE);
  }

  protected function clear($key, $wildcard = FALSE) {
    cache_clear_all($key, $this->config['cache']['bin'], $wildcard);
  }
}
