<?php

namespace AdimeoDataSuite\Index;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;

class StatIndexManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite_stat';

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

  /**
   * @return Client
   */
  public function getClient() {
    return $this->client;
  }

  public function saveStat($target, $facets = array(), $query = '', $analyzedQuery = '', $apiUrl = '', $resultCount = 0, $responseTime = 0, $remoteAddress = '', $tag = '')
  {
    try {
      $this->getClient()->search(array(
        'index' => static::APP_INDEX_NAME,
        'type' => 'stat',
        'body' => array(
          'query' => array(
            'match_all' => array(
              'boost' => 1
            )
          )
        )
      ));
    }
    catch(Missing404Exception $ex) {
      //stat index does not exist
      $this->client->indices()->create(array(
        'index' => static::APP_INDEX_NAME,
        'body' => []
      ));
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/stat_structure.json'), TRUE);
      $this->putMapping(static::APP_INDEX_NAME, 'stat', $json);
    }
    $indexName = strpos($target, '.') === 0 ? ('.' . explode('.', $target)[1]) : explode('.', $target)[0];
    $params = array(
      'index' => static::APP_INDEX_NAME,
      'type' => 'stat',
      'body' => array(
        'date' => date('Y-m-d\TH:i:s'),
        'index' => $indexName,
        'mapping' => $target,
        'remote_addr' => $remoteAddress,
        'log' => $tag,
        'facets' => $facets,
        'query' => array(
          'raw' => $query,
          'analyzed' => $analyzedQuery
        ),
        'api_url' => $apiUrl,
        'result_count' => $resultCount,
        'response_time' => $responseTime
      )
    );
    $r = $this->getClient()->index($params);
    $this->getClient()->indices()->flush();
    unset($params);
    return $r;
  }

  private function putMapping($indexName, $mappingName, $mapping) {
    $body = array(
      'properties' => $mapping
    );
    $this->client->indices()->putMapping(array(
      'index' => $indexName,
      'type' => $mappingName,
      'body' => $body
    ));
  }

  public function search($indexName, $query, $from = 0, $size = 20, $type = null) {
    $params = array(
      'index' => $indexName,
      'body' =>$query
    );
    if($type != null) {
      $params['type'] = $type;
    }
    $params['body']['from'] = $from;
    $params['body']['size'] = $size;
    return $this->client->search($params);
  }

}