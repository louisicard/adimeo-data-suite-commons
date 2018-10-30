<?php

namespace AdimeoDataSuite\Index;

use AdimeoDataSuite\Model\PersistentObject;
use AdimeoDataSuite\Model\SecurityContext;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;

class IndexManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite';
  const APP_RECO_INDEX_NAME = '.adimeo_data_suite_reco';

  /**
   * @var Client
   */
  private $client;

  public function __construct($elasticsearchServerUrl) {
    $clientBuilder = new ClientBuilder();
    if(!defined('JSON_PRESERVE_ZERO_FRACTION')){
      $clientBuilder->allowBadJSONSerialization();
    }
    $clientBuilder->setHosts(array($elasticsearchServerUrl));
    $this->client = $clientBuilder->build();
  }


  public function getServerInfo() {
    return array(
      'server_info' => $this->client->info(),
      'health' => $this->client->cluster()->health(),
      'stats' => $this->client->cluster()->stats(),
    );
  }

  public function getIndicesList(SecurityContext $context = NULL) {
    $mappings = $this->client->indices()->getMapping();
    $settings = $this->client->indices()->getSettings();
    $indices = $this->client->indices()->stats()['indices'];
    foreach($indices as $index => $stats) {
      if(isset($settings[$index])) {
        $indices[$index]['settings'] = $settings[$index]['settings'];
      }
      if(isset($mappings[$index])) {
        $indices[$index]['mappings'] = $mappings[$index]['mappings'];
      }
    }
    ksort($indices);
    if($context != NULL && !$context->isAdmin()) {
      foreach($indices as $k => $data) {
        if(!in_array($k, $context->getRestrictions()['indexes']))
          unset($indices[$k]);
      }
    }
    return $indices;
  }

  function getIndicesInfo(SecurityContext $context = NULL)
  {
    $info = array();
    $stats = $this->client->indices()->stats();
    foreach ($stats['indices'] as $index_name => $stat) {
      $info[$index_name] = array(
        'count' => $stat['total']['docs']['count'] - $stat['total']['docs']['deleted'],
        'size' => round($stat['total']['store']['size_in_bytes'] / 1024 / 1024, 2) . ' MB',
      );
      $mappings = $this->client->indices()->getMapping(array('index' => $index_name));
      foreach ($mappings[$index_name]['mappings'] as $mapping => $properties) {
        $info[$index_name]['mappings'][] = array(
          'name' => $mapping,
          'field_count' => count($properties['properties']),
        );
      }
    }
    ksort($info);
    if($context != NULL && !$context->isAdmin()) {
      foreach($info as $k => $data) {
        if(!in_array($k, $context->getRestrictions()['indexes']))
          unset($info[$k]);
      }
    }
    unset($stats);
    return $info;
  }

  public function getIndex($indexName) {
    try {
      return $this->client->indices()->getSettings(array('index' => $indexName));
    }
    catch(Missing404Exception $ex) {
      return null;
    }
  }

  public function createIndex($indexName, $settings) {
    if (isset($settings['creation_date']))
      unset($settings['creation_date']);
    if (isset($settings['version']))
      unset($settings['version']);
    if (isset($settings['uuid']))
      unset($settings['uuid']);
    if (isset($settings['provided_name']))
      unset($settings['provided_name']);
    $params = array(
      'index' => $indexName,
    );
    $settings['analysis']['analyzer']['transliterator'] = array(
      'filter' => array('standard', 'asciifolding', 'lowercase'),
      'tokenizer' => 'keyword'
    );
    if (count($settings) > 0) {
      $params['body'] = array(
        'settings' => $settings,
      );
    }
    return $this->client->indices()->create($params);
  }

  function updateIndex($indexName, $settings)
  {
    $this->client->indices()->close(array(
      'index' => $indexName
    ));
    if (isset($settings['creation_date']))
      unset($settings['creation_date']);
    if (isset($settings['version']))
      unset($settings['version']);
    if (isset($settings['uuid']))
      unset($settings['uuid']);
    if (isset($settings['number_of_shards']))
      unset($settings['number_of_shards']);
    if (isset($settings['number_of_replicas']))
      unset($settings['number_of_replicas']);
    //if (isset($settings['analysis']))
    //  unset($settings['analysis']);
    if (isset($settings['provided_name']))
      unset($settings['provided_name']);
    if (count($settings) > 0) {
      try {
        $this->client->indices()->putSettings(array(
          'index' => $indexName,
          'body' => array(
            'settings' => $settings,
          ),
        ));
      }
      catch(\Exception $ex) {

      }
    }
    $this->client->indices()->open(array(
      'index' => $indexName
    ));
  }

  public function deleteIndex($indexName) {
    $params = array(
      'index' => $indexName
    );
    return $this->client->indices()->delete($params);
  }

  /**
   *
   * @param string $indexName
   * @return string[]
   */
  function getAnalyzers($indexName)
  {
    $analyzers = array('standard', 'simple', 'whitespace', 'stop', 'keyword', 'pattern', 'language', 'snowball');
    $settings = $this->client->indices()->getSettings(array(
      'index' => $indexName,
    ));
    if (isset($settings[$indexName]['settings']['index']['analysis']['analyzer'])) {
      foreach ($settings[$indexName]['settings']['index']['analysis']['analyzer'] as $analyzer => $definition) {
        $analyzers[] = $analyzer;
      }
    }
    unset($settings);
    return $analyzers;
  }

  /**
   *
   * @return string[]
   */
  function getFieldTypes()
  {
    $types = array('integer', 'long', 'float', 'double', 'boolean', 'date', 'ip', 'geo_point');
    if($this->getServerMajorVersionNumber() >= 5){
      $types = array_merge($types, array('text', 'keyword'));
    }
    else{
      $types = array_merge($types, array('string'));
    }
    asort($types);
    return $types;
  }

  /**
   *
   * @return string[]
   */
  function getDateFormats()
  {
    return array('basic_date', 'basic_date_time', 'basic_date_time_no_millis', 'basic_ordinal_date', 'basic_ordinal_date_time', 'basic_ordinal_date_time_no_millis', 'basic_time', 'basic_time_no_millis', 'basic_t_time', 'basic_t_time_no_millis', 'basic_week_date', 'basic_week_date_time', 'basic_week_date_time_no_millis', 'date', 'date_hour', 'date_hour_minute', 'date_hour_minute_second', 'date_hour_minute_second_fraction', 'date_hour_minute_second_millis', 'date_optional_time', 'date_time', 'date_time_no_millis', 'hour', 'hour_minute', 'hour_minute_second', 'hour_minute_second_fraction', 'hour_minute_second_millis', 'ordinal_date', 'ordinal_date_time', 'ordinal_date_time_no_millis', 'time', 'time_no_millis', 't_time', 't_time_no_millis', 'week_date', 'week_date_time', 'weekDateTimeNoMillis', 'week_year', 'weekyearWeek', 'weekyearWeekDay', 'year', 'year_month', 'year_month_day');
  }

  function getServerMajorVersionNumber(){
    $info = $this->getServerInfo()['server_info'];
    return (int)explode('.', $info['version']['number'])[0];
  }

  public function putMapping($indexName, $mappingName, $mapping, $dynamicTemplates = NULL, $wipeData = false) {
    if ($wipeData) {
      $this->deleteByQuery($indexName, $mappingName, array(
        'query' => array(
          'match_all' => array('boost' => 1)
        )
      ));
    }

    $body = array(
      'properties' => $mapping
    );
    if($dynamicTemplates != NULL) {
      $body['dynamic_templates'] = $dynamicTemplates;
    }
    $this->client->indices()->putMapping(array(
      'index' => $indexName,
      'type' => $mappingName,
      'body' => $body
    ));
  }

  function getMapping($indexName, $mappingName)
  {
    try {
      $mapping = $this->client->indices()->getMapping(array(
        'index' => $indexName,
        'type' => $mappingName,
      ));
      if (isset($mapping[$indexName]['mappings'][$mappingName])) {
        return $mapping[$indexName]['mappings'][$mappingName];
      } else
        return null;
    } catch (\Exception $ex) {
      return null;
    }
  }

  public function initStore() {
    $indices = array_keys($this->getIndicesList());
    if(!in_array(static::APP_INDEX_NAME, $indices)) {
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/store_structure.json'), TRUE);
      $this->createIndex(static::APP_INDEX_NAME, $json['index']);
    }
    $mapping = $this->getMapping(static::APP_INDEX_NAME, 'store_item');
    if($mapping == null) {
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/store_structure.json'), TRUE);
      $this->putMapping(static::APP_INDEX_NAME, 'store_item', $json['mapping']);
    }
  }

  public function search($indexName, $query, $type = '') {
    return $this->client->search(array(
      'index' => $indexName,
      'type' => $type,
      'body' => $query
    ));
  }

  public function persistObject(PersistentObject $o) {
    $params = array(
      'index' => static::APP_INDEX_NAME,
      'type' => 'store_item',
      'body' => array(
        'name' => $o->getName(),
        'type' => $o->getType(),
        'created_by' => $o->getCreatedBy(),
        'tags' => $o->getTags(),
        'data' => $o->serialize()
      )
    );
    if($o->getId() != null) {
      $params['id'] = $o->getId();
    }
    $r = $this->client->index($params);
    if(isset($r['_id'])){
      $o->setId($r['_id']);
    }
    $this->client->indices()->flush();
    return $o;
  }

  public function deleteObject($id) {
    $this->client->delete(array(
      'index' => static::APP_INDEX_NAME,
      'type' => 'store_item',
      'id' => $id
    ));
    $this->client->indices()->flush();
  }

  public function findObject($type, $id) {
    $query = array(
      'query' => array(
        'bool' => array(
          'must' => array(
            array(
              'term' => array(
                'type' => $type
              )
            ),
            array(
              'ids' => array(
                'values' => array($id)
              )
            )
          )
        )
      )
    );
    $r = $this->search(static::APP_INDEX_NAME, $query);
    if(isset($r['hits']['hits'][0])) {
      $object = unserialize($r['hits']['hits'][0]['_source']['data']);
      $object->setId($r['hits']['hits'][0]['_id']);
      return $object;
    }
    else {
      return null;
    }
  }

  public function listObjects($type, SecurityContext $context = NULL, $from = 0, $size = 10000, $order = 'asc') {
    $query = array(
      'query' => array(
        'bool' => array(
          'should' => array(
            array(
              'bool' => array(
                'must' => [
                  array(
                    'term' => array(
                      'type' => $type
                    )
                  )
                ]
              )
            )
          )
        )
      ),
      'size' => $size,
      'from' => $from,
      'sort' => array(
        'name.raw' => $order
      )
    );
    if($context != null && !$context->isAdmin()) {
      $restricted = array(
        'datasource' => 'datasources',
        'matching_list' => 'matchingLists',
        //TODO: Potential bug = multiple parameters can share the same name in different contexts which could lead to major problems when executing datasources outside context
        'parameter' => 'parameters'
      );
      foreach($restricted as $restrictionType => $restriction) {
        if ($type == $restrictionType) {
          $query['query']['bool']['should'][0]['bool']['must'][] = array(
            'ids' => array(
              'values' => $context->getRestrictions()[$restriction]
            )
          );
        }
      }
      if($type == 'processor') {
        $procQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              ),
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['datasources'] as $procDs) {
          $procQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'datasource_id=' . $procDs
            )
          );
        }
        foreach($context->getRestrictions()['indexes'] as $procIndexes) {
          $procQuery['bool']['must'][1]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $procIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $procQuery;
      }
      if($type == 'search_page') {
        $spQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['indexes'] as $spIndexes) {
          $spQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $spIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $spQuery;
      }
      if($type == 'autopromote') {
        $spQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['indexes'] as $spIndexes) {
          $spQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $spIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $spQuery;
      }
      $query['query']['bool']['should'][] = array(
        'bool' => array(
          'must' => array(
            array(
              'term' => array(
                'created_by' => $context->getUserUid()
              )
            ),
            array(
              'term' => array(
                'type' => $type
              )
            )
          )
        )
      );
    }
    $r = $this->search(static::APP_INDEX_NAME, $query);
    $objects = [];
    foreach($r['hits']['hits'] as $hit) {
      $object = unserialize($hit['_source']['data']);
      $object->setId($hit['_id']);
      $objects[] = $object;
    }
    return $objects;
  }

  public function deleteByQuery($indexName, $mappingName, $query)
  {
    if($this->getServerMajorVersionNumber() >= 5) {
      $this->client->deleteByQuery(array(
        'index' => $indexName,
        'type' => $mappingName,
        'body' => $query
      ));
    }
    else{
      //Delete by query is not available on ES 2.x clusters so let's do it on our own
      $this->scroll($query, $indexName, $mappingName, function($items){
        $this->bulkDelete($items);
      },500);
    }
  }

  public function scroll($queryBody, $index, $mapping, $callback, $size = 10)
  {
    $r = $this->client->search(array(
      'index' => $index,
      'type' => $mapping,
      'body' => $queryBody,
      'scroll' => '10ms',
      'size' => $size
    ));
    if (isset($r['_scroll_id'])) {
      while (count($r['hits']['hits']) > 0) {
        $callback($r['hits']['hits']);
        $scrollId = $r['_scroll_id'];
        $r = $this->client->scroll(array(
          'scroll_id' => $scrollId,
          'scroll' => '1m'
        ));
      }
    }
  }

  public function bulkDelete($items)
  {
    $bulkString = '';
    foreach ($items as $item) {
      $data = array('delete' => array('_index' => $item['_index'], '_type' => $item['_type'], '_id' => $item['_id']));
      $bulkString .= json_encode($data) . "\n";
    }
    if (count($items) > 0) {
      $params['index'] = $items[0]['_index'];
      $params['type'] = $items[0]['_type'];
      $params['body'] = $bulkString;
      $this->client->bulk($params);
    }
  }

}