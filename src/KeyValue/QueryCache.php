<?php

namespace QueryCache\KeyValue;

use QueryCache\QueryCache as BaseQueryCache;

/**
 * A class for caching key-value like queries.
 */
class QueryCache extends BaseQueryCache
{

    /**
     * {@inheritdoc}
     */
    public function invalidateQueryCache($cacheable_query)
    {
        $key = $cacheable_query->getKVCacheKey();

        if ($key !== false) {
            $cache_pool = $this->cachePoolFactory->get($cacheable_query->getKVCacheConfiguration());
            $cache_pool->deleteItem($key);
        }

        parent::invalidateQueryCache($cacheable_query);
        return;
    }
}
