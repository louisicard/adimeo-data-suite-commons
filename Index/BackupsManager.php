<?php
namespace AdimeoDataSuite\Index;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;


class BackupsManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite';
  const APP_RECO_INDEX_NAME = '.adimeo_data_suite_reco';

  /**
   * @var Client
   */
  private $client;

  /**
   * BackupManager constructor.
   * @param $elasticsearchServerUrl
   */
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


  /**
   * Get all repositories
   *
   * @return array
   */
  public function getBackupRepositories()
  {
    $allRepositories = $this->getClient()->snapshot()->getRepository(array('repository' => '_all'));

    return $allRepositories;
  }

}