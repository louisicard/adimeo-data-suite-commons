<?php

namespace AdimeoDataSuite\Index;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;

class RecoIndexManager
{

  const RECO_INDEX_NAME = '.adimeo_data_suite_reco';

  /**
   * @var Client
   */
  private $client;

  private $indexNumberOfShards;

  private $indexNumberOfReplicas;

  public function __construct($elasticsearchServerUrl, $numberOfShards = 1, $numberOfReplicas = 1) {
    $clientBuilder = new ClientBuilder();
    if(!defined('JSON_PRESERVE_ZERO_FRACTION')){
      $clientBuilder->allowBadJSONSerialization();
    }
    $clientBuilder->setHosts(array($elasticsearchServerUrl));
    $this->client = $clientBuilder->build();

    $this->indexNumberOfShards = $numberOfShards;
    $this->indexNumberOfReplicas = $numberOfReplicas;
  }

  /**
   * @return Client
   */
  public function getClient() {
    return $this->client;
  }

  public function getRecoPath($path_id, $host)
  {
    try {
      $query = array(
        'index' => static::RECO_INDEX_NAME,
        'type' => 'path',
        'body' => array(
          'query' => array(
            'bool' => array(
              'must' => array(
                array(
                  'ids' => array(
                    'values' => array($path_id),
                  )
                ),
                array(
                  'term' => array(
                    'host' => $host
                  )
                )
              )
            )
          )
        )
      );
      $r = $this->getClient()->search($query);
      if (isset($r['hits']['hits']) && count($r['hits']['hits']) > 0) {
        return array(
            'id' => $r['hits']['hits'][0]['_id']
          ) + $r['hits']['hits'][0]['_source'];
      }
      else {
        return null;
      }
    } catch (\Exception $ex) {
      return null;
    }
  }

  public function getRecos($id, $host, $index, $mapping)
  {
    try {
      $query = array(
        'index' => static::RECO_INDEX_NAME,
        'type' => 'path',
        'body' => array(
          'size' => 0,
          'query' => array(
            'bool' => array(
              'must' => array(
                array(
                  'term' => array(
                    'ids' => $id,
                  )
                ),
                array(
                  'term' => array(
                    'host' => $host
                  )
                )
              )
            )
          ),
          'aggs' => array(
            'ids' => array(
              "terms" => array(
                "field" => "ids",
                "size" => 20,
              )
            )
          )
        )
      );
      $r = $this->getClient()->search($query);
      if (isset($r['aggregations']['ids']['buckets'])) {
        $ids = array();
        foreach ($r['aggregations']['ids']['buckets'] as $bucket) {
          if ($bucket['key'] != $id) {
            $ids[$bucket['key']] = array();
          }
        }
        if (count($ids) > 0) {
          $r = $this->getClient()->search(array(
            'index' => $index,
            'type' => $mapping,
            'body' => array(
              'size' => 20,
              'query' => array(
                'ids' => array(
                  'values' => array_keys($ids)
                )
              )
            )
          ));
          if (isset($r['hits']['hits'])) {
            foreach ($r['hits']['hits'] as $hit) {
              if (isset($ids[$hit['_id']])) {
                $ids[$hit['_id']] = $hit['_source'];
              }
            }
          }
          foreach ($ids as $k => $data) {
            if (empty($data)) {
              unset($ids[$k]);
            }
          }
        }
        return $ids;
      }
      else {
        return array();
      }
    } catch (\Exception $ex) {
      return array();
    }
  }

  public function saveRecoPath($path)
  {
    try {
      $this->getClient()->search(array(
        'index' => static::RECO_INDEX_NAME,
        'type' => 'path',
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
      //reco index does not exist
      $this->getClient()->indices()->create(array(
        'index' => static::RECO_INDEX_NAME,
        'body' => [
          'settings' => [
            'number_of_shards' => $this->indexNumberOfShards,
            'number_of_replicas' => $this->indexNumberOfReplicas,
          ]
        ]
      ));
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/reco_structure.json'), TRUE);
      $this->putMapping(static::RECO_INDEX_NAME, 'path', $json);
    }
    $params = array(
      'index' => IndexManager::APP_RECO_INDEX_NAME,
      'type' => 'path',
      'id' => $path['id'],
      'body' => array(
        'host' => $path['host'],
        'ids' => $path['ids'],
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
}