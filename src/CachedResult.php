<?php

/**
 * @file
 * Contains \Drupal\query_cache\CachedResult.
 */

namespace Drupal\query_cache;

class CachedResult {

  protected $data;
  protected $filter;
  protected $index;
  protected $count;

  public function __construct($data = array(), $filter = array()) {
    $this->data = $data;
    $this->filter = array_flip($filter);

    $this->index = 0;
    $this->count = count($data);
  }

  protected function fetchRow($index) {
    $record = $this->data[$index];
    if (!empty($this->filter)) {
      $record = array_intersect_key($record, $this->filter);
    }

    return $record;
  }

  public function fetchArray() {
    if ($this->index < $this->count) {
      $result = $this->fetchRow($this->index);
      $this->index++;
      return $result;
    }

    return FALSE;
  }

  public function fetchObject() {
    if ($this->index < $this->count) {
      return (object) $this->fetchArray();
    }

    return FALSE;
  }

  public function result() {
    if ($this->count == 0) {
      return FALSE;
    }

    $row = $this->fetchRow(0);
    $result = array_shift($row);

    return $result;
  }
}
