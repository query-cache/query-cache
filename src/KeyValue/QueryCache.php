<?php

namespace QueryCache\KeyValue;

use QueryCache\QueryCache as BaseQueryCache;

/**
 * A class for caching key-value like queries.
 */
class QueryCache extends BaseQueryCache
{

    public function cacheMiddleware($callbacks, $query, $args, $options, $table_config)
    {
        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);

        $key = $cacheable_query->getKVCacheKey();

        if ($key === false) {
            return parent::executeCacheableQuery($callbacks, $query, $args, $options, $table_config);
        }

        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getKVCacheConfiguration());

        $keys = array($key);
        $items = $cache_pool->getMultiple($keys);

        if (!empty($items)) {
            return $items[0];
        }

        list($query, $args, $options) = $cacheable_query->getKVQueryArgsOptions();
        $data = $this->query($query, $args, $options);
        $cache_pool->set($key, $data);

        return $data;
    }

    /**
     * {@inheritdoc}
     */
    public function invalidateQueryCache($query, $args, $options, $table_config)
    {
        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);

        $key = $cacheable_query->getKVCacheKey();
        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getKVCacheConfiguration());

        if ($key !== false) {
            $cache_pool->deleteItem($key);
        } elseif ($cacheable_query->getQueryType() != 'INSERT') {
            // @todo Warn when this happens.
            $cache_pool->clear();
        }

        parent::invalidateQueryCache($cacheable_query);
        return;
    }
}
