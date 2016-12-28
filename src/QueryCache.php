<?php

namespace QueryCache;

/**
 * A class for caching queries.
 */
class QueryCache implements QueryExecutorInterface
{
    protected $config;
    protected $queryExecutor;
    protected $cachePoolFactory;
    protected $cacheableQueryClass = '\QueryCache\CacheableQuery';

    public function __construct($config, $query_executor, $cache_pool_factory)
    {
        $this->config = $config;
        $this->queryExecutor = $query_executor;
        $this->cachePoolFactory = $cache_pool_factory;
    }

    /**
     * Executes the given query with the argument and options.
     *
     * @param string $query
     * @param array $args
     * @param array $options
     *
     * @return array|\Traversable
     */
    public function query($query, $args, $options)
    {
	// Early return if this table is not cacheable.
        $table_config = $this->getQueryTableConfiguration($query);
        if (!$table_config) {
            return $this->queryExecutor->query($query, $args, $options);
        }

        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);

        $query_type = $cacheable_query->queryType();

        // Invalidate the query cache for CUD operations.
        if ($query_type == 'INSERT' || $query_type == 'UPDATE' || $query_type == 'DELETE') {
            $this->invalidateQueryCache($cacheable_query);
        }

        // If this is not a SELECT query or not cacheable, then execute the
        // original query and return.
        if ($query_type != 'SELECT' || !$cacheable_query->isCacheable()) {
            return $this->queryExecutor->query($query, $args, $options);
        }

        return $this->executeCacheableQuery($cacheable_query);
    }

    public function executeCacheableQuery($cacheable_query)
    {
        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getCacheConfiguration());

        $key = $cacheable_query->getCacheKey();
        $keys = array($key);

        $items = $cache_pool->getMultiple($keys);

        if (!empty($items)) {
            return $items[0];
        }

        $data = $this->queryExecutor->query($query, $args, $options);
        $cache_pool->set($key, $data);

        return $data;
    }

    public function invalidateQueryCache($cacheable_query)
    {
        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getCacheConfiguration());
        $cache_pool->clear();
    }

    protected function getQueryTableConfiguration($query)
    {
        $query_table = null;

        // Find table used.
        if (isset($this->config['queries'][$query])) {
            $query_table = $this->config['queries'][$query];
        } else {
            $t = explode('{', $query);

            // Check that there is exactly one table.
            if (isset($t[1]) && count($t) == 2) {
                list($query_table) = explode('}', $t[1], 2);
            }
        }

        // If table could not be found, return early.
        if (isset($query_table) && isset($this->config['tables'][$query_table])) {
            return $this->config['tables'][$query_table];
        }

        return false;
    }
}
