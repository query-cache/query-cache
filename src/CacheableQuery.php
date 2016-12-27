<?php

namespace QueryCache;

/**
 * A class for caching queries.
 */
class CacheableQuery extends Query {

  public function execute() {
    // If the query is opt-ed out from caching, return.
    if (isset($this->queryInfo) && empty($this->queryInfo['cache'])) {
      return parent::execute();
    }

    // Unless we cache all queries, ensure that the query is known.
    if (empty($this->config['cache_all_queries']) && !isset($this->queryInfo)) {
      return parent::execute();
    }

    return $this->executeCachedQuery();
  }

  /**
   * Returns the query cache key for the query.
   *
   * @return string
   *   The query cache key.
   */
  protected function getQueryCacheKey() {
    $keys = array();
    $keys[] = $this->queryCacheKeyPrefix();
    $keys[] = $this->query;

    foreach ($this->namedArguments() as $name => $arg) {
      if (is_array($arg)) {
        $arg = implode(',', $arg);
      }
      $keys[] = $name . '=' . $arg;
    }

    return implode(':', $keys);
  }

  protected function executeCachedQuery() {
    $cid = $this->getQueryCacheKey();

    $cache = $this->cacheGet($cid);
    if ($cache) {
      return new CachedResult($cache->data);
    }

    $result = $this->executeOriginalQuery();

    $data = array();
    while ($row = db_fetch_array($result)) {
      $data[] = $row;
    }

    $this->cacheSet($cid, $data);

    return new CachedResult($data);
  }

  protected function cacheGet($cid) {
    return cache_get($cid, $this->config['cache']['bin']);
  }

  protected function cacheSet($cid, $data) {
    cache_set($cid, $data, $this->config['cache']['bin'], $this->config['cache']['expire']);
  }
}
