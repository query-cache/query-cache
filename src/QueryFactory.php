<?php

namespace QueryCache;

/**
 * A class for creating query objects.
 */
class QueryFactory {

  public static function create($query, $args, $config) {
    $query_type = Query::queryType($query);
    if ($query_type == 'INSERT' || $query_type == 'UPDATE' || $query_type == 'DELETE') {
      return new CUDQuery($query, $args, $config);
    }

    if ($query_type == 'SELECT') {
      return new CacheableQuery($query, $args, $config);
    }

    // Fallback to execute the original query.
    return new Query($query, $args, $config);
  }

}
