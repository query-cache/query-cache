<?php

namespace QueryCache\KeyValue;

use QueryCache\CUDQuery as BaseCUDQuery;

/**
 * A class for invalidating key-value queries on CUD operations.
 */
class CUDQuery extends BaseCUDQuery
{

    /**
     * {@inheritdoc}
     */
    public function invalidateCache()
    {
        $columns = $this->config['primary_key'];
        $key = $this->getKVCacheKey();

        if (empty($columns) || $key === false) {
            $this->clearAll();
            return;
        }

        $this->clear($key);
        $this->clear($this->queryCacheKeyPrefix() . ':', true);
    }
}