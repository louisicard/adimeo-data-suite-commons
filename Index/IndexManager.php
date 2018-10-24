<?php

namespace AdimeoDataSuite\Bundle\CommonsBundle\Index;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

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

  /**
   * @return Client
   */
  public function getClient() {
    return $this->client;
  }

  public function getServerInfo()
  {
    return $this->getClient()->info();
  }

}
