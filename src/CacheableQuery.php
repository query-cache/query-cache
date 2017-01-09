<?php

namespace QueryCache;

/**
 * A base class for caching and invalidating queries.
 */
class CacheableQuery
{

    protected $query;
    protected $args;
    protected $options;

    protected $config;
    protected $queryInfo;
    protected $cacheable;
    protected $namedArgs;

  /**
   * Constructs a Query object.
   *
   * @param string $query
   *   The query to execute.
   * @param array $args
   *   The arguments for the query.
   * @param array $options
   *   An array of options to control how the query operates.
   * @param array $config
   *   The configuration for the table the query affects.
   */
    public function __construct($query, $args, $options, $config)
    {
        $this->query = $query;
        $this->args = $args;
        $this->options = $options;
        $this->config = $config;

        $this->cacheable = !empty($this->config['cache']['all_queries']);

        if (isset($this->config['queries'][$this->query])) {
            $this->queryInfo = $this->config['queries'][$this->query];
            $this->cacheable = !empty($this->queryInfo['cache']);
        }

        $this->namedArgs = static::namedArguments($this->args, $this->queryInfo);
    }

    public function isCacheable()
    {
        return $this->cacheable;
    }

    public function getQueryArgsOptions()
    {
        return array($this->query, $this->args, $this->options);
    }

    /**
     * Returns the cache configuration.
     *
     * @return array
     *   The cache configuration.
     */
    public function getCacheConfiguration()
    {
        return $this->config['cache'];
    }

    /**
     * Returns the query cache key for the query.
     *
     * @return string
     *   The query cache key.
     */
    public function getCacheKey()
    {
        $keys = array();
        $keys[] = 'query';
        $keys[] = $this->query;

        foreach ($this->namedArgs as $name => $arg) {
            if (is_array($arg)) {
                $arg = implode(',', $arg);
            }
            $keys[] = $name . '=' . $arg;
        }

        return implode(':', $keys);
    }

    public static function queryType($query)
    {
        list($type) = explode(' ', $query);
        return strtoupper($type);
    }

    public static function namedArguments($args, $query_info)
    {
        // Fallback to arg_? in case of unnamed arguments.
        if (isset($query_info['args'])) {
            $names = $query_info['args'];
        } else {
            $names = array();
            foreach ($args as $key => $val) {
                if (is_numeric($key)) {
                    $names[] = 'arg_' . $key;
                } else {
                    $names[] = $key;
                }
            }
        }

        return array_combine($names, $args);
    }


    /**
     * Returns the key-value cache key for the query or FALSE.
     *
     * @return string|false
     *   The cache key if the primary key is present in the arguments, FALSE
     *   otherwise.
     */
    public function getKVCacheKey()
    {
        if (empty($this->config['key_value']['cache'])) {
            return false;
        }
        elseif (isset($this->queryInfo['key_value']['cache']) && $this->queryInfo['key_value']['cache'] === false) {
            return false;
        }

        $keys = array();
        $keys[] = 'key_value';

        foreach ($this->config['key_value']['key'] as $key) {
            if (!isset($this->namedArgs[$key])) {
                return false;
            }
            $arg = $this->namedArgs[$key];
            if (is_array($arg)) {
                $arg = implode(',', $arg);
            }

            $keys[] = $key . '=' . $arg;
        }

        if (count($keys) != (count($this->namedArgs) + 1)) {
            return false;
        }

        return implode(':', $keys);
    }

    /**
     * Returns the cache configuration for the key_value store.
     *
     * @return array
     *   The key_value cache configuration.
     */
    public function getKVCacheConfiguration()
    {
        return $this->config['key_value']['cache'];
    }

    public function getKVQueryArgsOptions()
    {
        $args = array();

        foreach ($this->config['key_value']['args'] as $key) {
            $args[] = $this->namedArgs[$key];
        }

        return array($this->config['key_value']['query'], $args, $this->options);
    }
}
