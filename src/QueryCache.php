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
        $query_type = $class::queryType($query);

        // Invalidate the query cache for CUD operations.
        if ($query_type == 'INSERT' || $query_type == 'UPDATE' || $query_type == 'DELETE') {
            $result = $this->queryExecutor->query($query, $args, $options);

            $cacheable_query = new $class($query, $args, $options, $table_config);
            $this->invalidateQueryCache($cacheable_query);
            return $result;
        }

        // If this is not a SELECT query, then execute the original query and
        // return.
        if ($query_type != 'SELECT') {
            return $this->queryExecutor->query($query, $args, $options);
        }

        $test_query = false;
        $random = mt_rand(1, 100);
        if (!empty($table_config['test_queries']) && $random <= $table_config['test_queries']) {
            $test_query = $query;
            $test_args = $args;
            $test_options = $options;
            $pre_data = $this->queryExecutor->query($query, $args, $options);
        }

        // Check for map/reduce
        $query_info = null;
        $filter = null;
        if (isset($table_config['queries'][$query])) {
            $query_info = $table_config['queries'][$query];
        }

        if (isset($query_info['map_reduce']['query'])) {
            $query = $query_info['map_reduce']['query'];
            $named_args = $class::namedArguments($args, $query_info);

            $args = array();
            foreach ($query_info['map_reduce']['args'] as $key) {
                $args[] = $named_args[$key];
            }
            if (!empty($query_info['map_reduce']['filter'])) {
                $filter = $query_info['map_reduce']['filter'];
            }
        }

        // Execute potentially cacheable query.
        $cacheable_query = new $class($query, $args, $options, $table_config);
        $data = $this->executeCacheableQuery($cacheable_query);

        if (!empty($data) && isset($filter)) {
            $data = static::applyFilter($data, $filter, $named_args);
        }

        if (!empty($test_query)) {
            if ($pre_data != $data) {
                $post_data = $this->queryExecutor->query($test_query, $test_args, $test_options);
                // Check $pre_data vs. $post_data to avoid race conditions.
                if ($pre_data == $post_data) {
                    // Trigger an erorr that the test failed.
                    trigger_error("Warning: Query $query failed test check.", E_USER_ERROR);
                }
            }
        }

        return $data;
    }

    public function executeCacheableQuery($cacheable_query)
    {
        if (!$cacheable_query->isCacheable()) {
            list($query, $args, $options) = $cacheable_query->getQueryArgsOptions();
            return $this->queryExecutor->query($query, $args, $options);
        }

        $cache_pool = $this->cachePoolFactory->get($cacheable_query->getCacheConfiguration());

        $key = $cacheable_query->getCacheKey();
        $keys = array($key);

        $items = $cache_pool->getMultiple($keys);

        if (!empty($items)) {
            return $items[0];
        }

        list($query, $args, $options) = $cacheable_query->getQueryArgsOptions();
        $data = $this->queryExecutor->query($query, $args, $options);
        $cache_pool->set($key, $data);

        return $data;
    }

    public function invalidateQueryCache($cacheable_query)
    {
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
                $tmp[$key] = $row[$field];
            }

            $args[] = $tmp;
            $args[] = $options[0];
            $args[] = $options[1];
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
                'cache_all_queries' => true,
                'test_queries' => false,
                // Experimental options.
                'key_value' => false,
            );

            $configuration['cache'] += array(
                'bin' => 'cache_query_' . $table,
                'keys' => array(),
                'expire' => -1, // @todo Add constant back.
                'tags' => array(),
            );

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
                    'bin' => 'cache_key_value_' . $table,
                    'keys' => array(),
                    'expire' => -1, // @todo Add constant back.
                    'tags' => array(),
                    );

                    $bin = $configuration['key_value']['cache']['bin'];
                    $query_cache_configuration['cache_bins'][$bin] = $bin;
                }
            }

            $bin = $configuration['cache']['bin'];
            $query_cache_configuration['cache_bins'][$bin] = $bin;

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
