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

    public function __construct($query_executor, $cache_pool_factory)
    {
        $this->queryExecutor = $query_executor;
        $this->cachePoolFactory = $cache_pool_factory;
    }

    public function setConfiguration($configuration)
    {
        $this->config = static::parseConfiguration($configuration);
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

    /**
     */
    protected static function parseConfiguration(array $base_configuration)
    {
        $query_cache_configuration = array();

        foreach ($base_configuration as $table => $configuration) {
            if (!$configuration) {
                    continue;
            }

            if ($configuration === true) {
                    $configuration = array();
            }

            $configuration += array(
                'cache' => array(),
                'queries' => array(),
                'cache_all_queries' => true,
                'test_queries' => false,
                // Experimental options.
                'key_value' => false,
                'primary_key' => array(),
            );

            $configuration['cache'] += array(
                'bin' => 'cache_query_' . $table,
                'keys' => array(),
                'expire' => -1, // @todo Add constant back.
                'tags' => array(),
            );

            $bin = $configuration['cache']['bin'];
            $queries = $configuration['queries'];
            $configuration['queries'] = array();

            foreach ($queries as $query_info) {
                $query_info += array(
                    'table' => $table,
                    'query' => '',
                    'args' => array(),
                    'cache' => true,
                    // Experimental options.
                    'tables' => array(),
                    'map_reduce' => array(),
                );

                $query = $query_info['query'];

                // Key queries by query for easier lookup.
                $configuration['queries'][$query] = $query_info;
                $query_cache_configuration['queries'][$query] = $table;
            }

            $query_cache_configuration['tables'][$table] = $configuration;
            $query_cache_configuration['cache_bins'][$bin] = $bin;
        }

        return $query_cache_configuration;
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

        // If table exists and is configured, return the configuration.
        if (isset($query_table) && isset($this->config['tables'][$query_table])) {
            return $this->config['tables'][$query_table];
        }

        return false;
    }
}
