<?php

namespace QueryCache;

/**
 * A class for invalidating queries on CREATE, UPDATE, DELETE operations.
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
