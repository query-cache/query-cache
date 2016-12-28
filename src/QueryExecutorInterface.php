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
     * @param string $query
     *   The query to execute.
     * @param array $args
     *   The arguments for the query.
     * @param array $options
     *   An array of options to control how the query operates.
     *
     * @return array|\Traversable
     */
    public function query($query, $args, $options);
}
