<?php

namespace QueryCache\KeyValue;

use QueryCache\QueryCache as BaseQueryCache;

/**
 * A class for caching key-value like queries.
 */
class QueryCache extends BaseQueryCache
{
    public function __construct($query_executor, $cache_pool_factory)
    {
        parent::__construct($query_executor, $cache_pool_factory);

        $temp = $this->CUDMiddlewares['cache'];
        unset($this->CUDMiddlewares['cache']);
        $this->CUDMiddlewares['cache_key_value'] = array($this, 'invalidateKVCacheMiddleware');
        $this->CUDMiddlewares['cache'] = $temp;

        $temp = $this->selectMiddlewares['cache'];
        unset($this->selectMiddlewares['cache']);
        $this->selectMiddlewares['cache_key_value'] = array($this, 'KVCacheMiddleware');
        $this->selectMiddlewares['cache'] = $temp;

    }

    public function KVCacheMiddleware($callbacks, $query, $args, $options, $table_config)
    {
        $callback = array_shift($callbacks);

        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);

        $key = $cacheable_query->getKVCacheKey();

        // Early return if this is not a key-value query.
        if ($key === false) {
            return $callback($callbacks, $query, $args, $options, $table_config);
        }

        // Get the data from the K/V query cache.
        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getKVCacheConfiguration());

        $keys = array($key);
        $items = $cache_pool->getMultiple($keys);

        if (!empty($items)) {
            return $items[0];
        }

        $data = $callback($callbacks, $query, $args, $options, $table_config);
        $cache_pool->set($key, $data);

        return $data;
    }

    /**
     * {@inheritdoc}
     */
    public function invalidateKVCacheMiddleware($callbacks, $query, $args, $options, $table_config)
    {
        $callback = array_shift($callbacks);

        $data = $callback($callbacks, $query, $args, $options, $table_config);

        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);

        $cache_config = $cacheable_query->getKVCacheConfiguration();
        if (!$cache_config) {
            return $data;
        }

        $key = $cacheable_query->getKVCacheKey();
        $cache_pool = $this->cachePoolFactory->get($cache_config);

        if ($key !== false) {
            $cache_pool->deleteItem($key);
        } elseif ($class::queryType($query) != 'INSERT') {
            // @todo Warn when this happens.
            $cache_pool->clear();
        }

        return $data;
    }
}
