<?php

namespace AdimeoDataSuite\Client;

use GuzzleHttp\Client;

class ElasticsearchClient
{

  private $elasticsearchServerUrl;

  /**
   * @var Client
   */
  private $client;

  public function __construct($elasticsearchServerUrl)
  {
    $this->elasticsearchServerUrl = $elasticsearchServerUrl;
    $this->client = new Client();
  }

}