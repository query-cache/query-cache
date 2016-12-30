<?php

namespace QueryCache;

/**
 * Interface for executing queries.
 */
interface QueryExecutorInterface
{

    /**
     * Executes the given query with the arguments and options.
     *
     * For SELECT queries the return value must be a \Traversable or array. For
     * non-SELECT queries the original value may be returned.
     *
     * @param string $query
     *   The query to execute.
     * @param array $args
     *   The arguments for the query.
     * @param array $options
     *   An array of options to control how the query operates.
     *
     * @return array|\Traversable|mixed
     */
    public function query($query, $args, $options);
}
