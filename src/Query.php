<?php

namespace QueryCache;

/**
 * A base class for caching and invalidating queries.
 */
class Query {

  protected $query;
  protected $args;
  protected $options;

  protected $config;
  protected $queryInfo;

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
  public function __construct($query, $args, $options, $config) {
    $this->query = $query;
    $this->args = $args;
    $this->options = $options;
    $this->config = $config;

    if (isset($this->config['queries'][$this->query])) {
      $this->queryInfo = $this->config['queries'][$this->query];
    }
  }

  /**
   * Executes the query.
   *
   * @return mixed
   *   A database query result, a CachedResult object, or FALSE if the
   *   query was not executed correctly.
   */
  public function execute() {
    return $this->executeOriginalQuery();
  }

  public function namedArguments() {
    // Fallback to arg_? in case of unnamed arguments.
    if (isset($this->queryInfo['args'])) {
      $names = $this->queryInfo['args'];
    }
    else {
      $names = array();
      foreach ($this->args as $key => $val) {
        if (is_numeric($key)) {
          $names[] = 'arg_' . $key;
        }
        else {
          $names[] = $key;
        }
      }
    }

    return array_combine($names, $this->args);
  }

  public function queryCacheKeyPrefix() {
    $keys = $this->config['cache']['keys'];
    $keys[] = 'query';
    return implode(':', $keys);
  }

  /**
   * Returns the key-value cache key for the query or FALSE.
   *
   * @return string|FALSE
   *   The cache key if the primary key is present in the arguments, FALSE
   *   otherwise.
   */
  public function getKVCacheKey() {
    $keys = $this->config['cache']['keys'];
    $keys[] = 'key_value';

    $named_args = $this->namedArguments();

    foreach ($this->config['primary_key'] as $key) {
      if (!isset($named_args[$key])) {
        return FALSE;
      }

      $keys[] = $key . '.' . $named_args[$key];
    }

    return implode(':', $keys);
  }

  public static function queryType($query) {
    list($type) = explode(' ', $query);
    return strtoupper($type);
  }

  /**
   * Executes the original query.
   */
  protected function executeOriginalQuery() {
    return $this->executeDBQuery($this->query, $this->args, $this->options);
  }

  /**
   * Executes a given database query.
   *
   * @param string $query
   * @param array $args
   * @param array $options
   */
  protected function executeDBQuery($query, $args, $options) {
    return _query_cache_execute_db_query($query, $args, $options);
  }
}
