<?php

namespace AdimeoDataSuite\Index;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class IndexManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite';

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
    $indexName = strpos($target, '.') === 0 ? ('.' . explode('.', $target)[1]) : explode('.', $target)[0];
    $params = array(
      'index' => IndexManager::APP_INDEX_NAME,
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

}