<?php

namespace QueryCache;

/**
 * A class for caching queries.
 */
class QueryCache implements QueryExecutorInterface
{
    protected $queryExecutor;
    protected $cachePoolFactory;

    protected $cacheableQueryClass = '\QueryCache\CacheableQuery';
    protected $selectMiddlewares = array();
    protected $config;

    public function __construct($query_executor, $cache_pool_factory)
    {
        $this->queryExecutor = $query_executor;
        $this->cachePoolFactory = $cache_pool_factory;

        $this->selectMiddlewares = array(
            'test_queries' => array($this, 'testQueryMiddleware'),
            'map_reduce' => array($this, 'mapReduceMiddleware'),
            'cache' => array($this, 'cacheMiddleware'),
        );
    }

    /**
     * {@inheritdoc}
     */
    public function query($query, $args, $options)
    {
        // Early return if this table is not cacheable.
        $table_config = $this->getQueryTableConfiguration($query);
        if (!$table_config) {
            return $this->queryExecutor->query($query, $args, $options);
        }

        $class = $this->cacheableQueryClass;
        $query_type = $class::queryType($query);

        // Invalidate the query cache for CUD operations.
        if ($query_type == 'INSERT' || $query_type == 'UPDATE' || $query_type == 'DELETE') {
            return $this->invalidateQueryCache($query, $args, $options, $table_config);
        }

        // If this is not a SELECT query, then execute the original query and
        // return.
        if ($query_type != 'SELECT') {
            return $this->queryExecutor->query($query, $args, $options);
        }

        $callbacks = $this->selectMiddlewares;
        $callbacks['queries'] = array($this, 'queryFinal');

        $callback = array_shift($callbacks);
        $data = $callback($callbacks, $query, $args, $options, $table_config);

        return $data;
    }

    public function setConfiguration($configuration)
    {
        $this->config = static::parseConfiguration($configuration);
    }

    public function getCachePools() {
        return $this->config['cache_pools'];
    }

    public function queryFinal($callbacks, $query, $args, $options, $table_config)
    {
            // assert('empty($callbacks)', 'Callbacks must be empty in the final callback.');
            return $this->queryExecutor->query($query, $args, $options);
    }

    public function testQueryMiddleware($callbacks, $query, $args, $options, $table_config)
    {
        $callback = array_shift($callbacks);
        $test_queries = null;
        $query_info = null;

        if (isset($table_config['queries'][$query])) {
            $query_info = $table_config['queries'][$query];
        }

        // Check for a query-specific override.
        if (isset($query_info['test_queries'])) {
            $test_queries = $query_info['test_queries'];
        }
        elseif (isset($table_config['test_queries'])) {
            $test_queries = $table_config['test_queries'];
        }

        // Early return if we do not want testing.
        if (empty($test_queries) || mt_rand(1, 100) > $test_queries) {
            return $callback($callbacks, $query, $args, $options, $table_config);
        }

        $pre_data = $this->queryExecutor->query($query, $args, $options);
        if ($pre_data instanceof \Traversable) {
            $pre_data = iterator_to_array($data);
        }

        $data = $callback($callbacks, $query, $args, $options, $table_config);
        if ($data instanceof \Traversable) {
            $data = iterator_to_array($data);
        }

        if ($pre_data != $data) {
            $post_data = $this->queryExecutor->query($query, $args, $options);
            if ($post_data instanceof \Traversable) {
                $post_data = iterator_to_array($data);
            }

            // Check $pre_data vs. $post_data to avoid race conditions.
            if ($pre_data == $post_data) {
                // Trigger an error that the test failed.
                trigger_error("Warning: Query $query failed test check.", E_USER_ERROR);
            }

        }

        return $data;
    }

    public function mapReduceMiddleware($callbacks, $query, $args, $options, $table_config)
    {
        $callback = array_shift($callbacks);

        // Check for map/reduce
        $query_info = null;
        $filter = null;
        if (isset($table_config['queries'][$query])) {
            $query_info = $table_config['queries'][$query];
        }

        if (isset($query_info['map_reduce']['query'])) {
            $query = $query_info['map_reduce']['query'];
            $class = $this->cacheableQueryClass;
            $named_args = $class::namedArguments($args, $query_info);

            $args = array();
            foreach ($query_info['map_reduce']['args'] as $key) {
                $args[] = $named_args[$key];
            }

            $data = $this->query($query, $args, $options);

            if (!empty($data) && !empty($query_info['map_reduce']['filter'])) {
                $filter = $query_info['map_reduce']['filter'];
                $data = static::applyFilter($data, $filter, $named_args);
            }

            return $data;
        }

        $data = $callback($callbacks, $query, $args, $options, $table_config);

        return $data;
    }

    public function cacheMiddleware($callbacks, $query, $args, $options, $table_config)
    {
        $callback = array_shift($callbacks);

        // Execute potentially cacheable query.
        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);

        if (!$cacheable_query->isCacheable()) {
            return $callback($callbacks, $query, $args, $options, $table_config);
        }

        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getCacheConfiguration());

        $key = $cacheable_query->getCacheKey();
        $keys = array($key);

        $items = $cache_pool->getMultiple($keys);

        if (!empty($items)) {
            return $items[0];
        }

        $data = $callback($callbacks, $query, $args, $options, $table_config);

        if ($data instanceof \Traversable) {
            $data = iterator_to_array($data);
        }

        $cache_pool->set($key, $data);

        return $data;
    }

    public function invalidateQueryCache($query, $args, $options, $table_config)
    {
        $class = $this->cacheableQueryClass;
        $cacheable_query = new $class($query, $args, $options, $table_config);
        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getCacheConfiguration());
        $cache_pool->clear();
    }

    public static function applyFilter($data, $filters, $named_arguments)
    {
        // First reduce the set.
        if (!empty($filters['where'])) {
            $conditions = array();
            foreach ($filters['where'] as $key => $value) {
                if (is_numeric($key)) {
                    $conditions[$value] = $named_arguments[$value];
                } else {
                    $conditions[$key] = $value;
                }
            }
            $new_data = array();
            foreach ($data as $row) {
                $row_valid = true;
                foreach ($conditions as $key => $value) {
                    if (!isset($row[$key]) || $row[$key] != $value) {
                        $row_valid = false;
                        break;
                    }
                }
                if ($row_valid) {
                    $new_data[] = $row;
                }
            }

            $data = $new_data;
        }
        // Order
        if (!empty($filters['order'])) {
            $order = array();

            foreach ($filters['order'] as $key => $value) {
                if (is_numeric($key)) {
                    $order[$value] = array(SORT_ASC, SORT_STRING);
                } else {
                    if (!is_array($value)) {
                        $value = array($value, SORT_STRING);
                    }
                    if (!isset($value[1])) {
                        $value = array($value[0], SORT_STRING);
                    }

                    $order[$key] = $value;
                }
            }

            $data = static::orderBy($data, $order);
        }

        // Select
        if (!empty($filters['select'])) {
            $select = $filters['select'];

            foreach ($data as $index => $row) {
                $new_row = array();
                foreach ($select as $key) {
                    $new_row[$key] = $row[$key];
                }

                $data[$index] = $new_row;
            }
        }

        return $data;
    }

    protected static function orderBy($data, $order)
    {
        foreach ($order as $field => $options) {
            $tmp = array();
            foreach ($data as $key => $row) {
                $tmp[$key] = str_replace('_', 'z', $row[$field]);
            }

            $args[] = $tmp;
            $args[] = $options[0];
            $args[] = $options[1] | SORT_FLAG_CASE;
        }

        $args[] = &$data;
        call_user_func_array('array_multisort', $args);
        return array_pop($args);
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
                // Experimental options.
                'key_value' => false,
            );

            $configuration['cache'] += array(
                'ttl' => false,
                'tags' => array(),
                'all_queries' => true,
            );
            $configuration['cache']['pool'] = 'query_' . $table;

            if ($configuration['key_value'] === true) {
                $configuration['key_value'] = array();
            }

            if ($configuration['key_value'] !== false) {
                $configuration['key_value'] += array(
                    'cache' => array(),
                    'key' => array(),
                    'query' => '',
                    'args' => array(),
                );

                if ($configuration['key_value']['cache'] !== false) {
                    $configuration['key_value']['cache'] += array(
                        'ttl' => false,
                        'tags' => array(),
                        'all_queries' => true,
                    );

                    $configuration['key_value']['cache']['pool'] = 'key_value_' . $table;

                    $bin = $configuration['key_value']['cache']['pool'];
                    $query_cache_configuration['cache_pools'][$bin] = $bin;
                }
            }

            $bin = $configuration['cache']['pool'];
            $query_cache_configuration['cache_pools'][$bin] = $bin;

            $queries = $configuration['queries'];
            $configuration['queries'] = array();

            foreach ($queries as $query_info) {
                $query_info += array(
                    'query' => '',
                    'args' => array(),
                    'cache' => true,
                    // Experimental options.
                    'tables' => array(), // @todo Implement me.
                    'map_reduce' => array(),
                );
                $query_info['table'] = $table;

                $query = $query_info['query'];

                // Key queries by query for easier lookup.
                $configuration['queries'][$query] = $query_info;
                $query_cache_configuration['queries'][$query] = $table;
            }

            $query_cache_configuration['tables'][$table] = $configuration;
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
