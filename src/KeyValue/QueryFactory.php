<?php

namespace QueryCache\KeyValue;

use QueryCache\QueryFactory as BaseQueryFactory;
use QueryCache\Query as BaseQuery;

/**
 * A class for creating query objects.
 */
class QueryFactory extends BaseQueryFactory
{

    /**
     * {@inheritdoc}
     */
    public static function create($query, $args, $options, $config)
    {
        if (empty($config['key_value'])) {
            return parent::create($query, $args, $options, $config);
        }

        $query_type = Query::queryType($query);
        if ($query_type == 'INSERT' || $query_type == 'UPDATE' || $query_type == 'DELETE') {
            return new CUDQuery($query, $args, $config);
        }

        if ($query_type == 'SELECT') {
            return new CacheableQuery($query, $args, $config);
        }

        // Fallback to execute the original query.
        return new BaseQuery($query, $args, $config);
    }
}
