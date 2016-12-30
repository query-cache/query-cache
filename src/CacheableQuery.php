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

        $this->cacheable = empty($this->config['cache_all_queries']);

        if (isset($this->config['queries'][$this->query])) {
            $this->queryInfo = $this->config['queries'][$this->query];
            $this->cacheable = !empty($this->queryInfo['cache']);
        }
    }

    public function isCacheable()
    {
        return $this->cacheable;
    }

    public function getQueryType()
    {
        list($type) = explode(' ', $this->query);
        return strtoupper($type);
    }

    public function getNamedArguments()
    {
        // Fallback to arg_? in case of unnamed arguments.
        if (isset($this->queryInfo['args'])) {
            $names = $this->queryInfo['args'];
        } else {
            $names = array();
            foreach ($this->args as $key => $val) {
                if (is_numeric($key)) {
                    $names[] = 'arg_' . $key;
                } else {
                    $names[] = $key;
                }
            }
        }

        return array_combine($names, $this->args);
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

        foreach ($this->getNamedArguments() as $name => $arg) {
            if (is_array($arg)) {
                $arg = implode(',', $arg);
            }
            $keys[] = $name . '=' . $arg;
        }

        return implode(':', $keys);
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
        $keys = array();
        $keys[] = 'key_value';

        $named_args = $this->getNamedArguments();

        foreach ($this->config['key_value']['key'] as $key) {
            if (!isset($named_args[$key])) {
                return false;
            }
            $arg = $named_args[$key];
            if (is_array($arg)) {
                $arg = implode(',', $arg);
            }

            $keys[] = $key . '=' . $arg;
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
        $named_args = $this->getNamedArguments();

        foreach ($this->config['key_value']['args'] as $key) {
            $args[] = $named_args[$key];
        }

        return array($this->config['key_value']['query'], $args, $this->options);
    }
}
