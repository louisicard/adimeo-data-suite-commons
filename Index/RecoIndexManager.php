<?php

namespace AdimeoDataSuite\Index;

use AdimeoDataSuite\Exception\ServerClientException;

class RecoIndexManager
{

  const RECO_INDEX_NAME = '.adimeo_data_suite_reco';

  private $indexNumberOfShards;

  private $indexNumberOfReplicas;

  /**
   * @var ServerClient
   */
  private $serverClient;

  /**
   * @var bool
   */
  private $isLegacy;

  public function __construct($elasticsearchServerUrl, $numberOfShards = 1, $numberOfReplicas = 1, $isLegacy = false) {

    $this->indexNumberOfShards = $numberOfShards;
    $this->indexNumberOfReplicas = $numberOfReplicas;
    $this->isLegacy = $isLegacy;
    $this->serverClient = new ServerClient($elasticsearchServerUrl);
  }

  /**
   * @return ServerClient
   */
  public function getServerClient() {
    return $this->serverClient;
  }

  public function isLegacy() {
    return $this->isLegacy;
  }

  public function getRecoPath($path_id, $host)
  {
    try {
      $query = array(
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
      );
      $r = $this->getServerClient()->search(static::RECO_INDEX_NAME, $query, $this->isLegacy() ? 'path' : null);
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
      );
      $r = $this->getServerClient()->search(static::RECO_INDEX_NAME, $query, $this->isLegacy() ? 'path' : null);
      if (isset($r['aggregations']['ids']['buckets'])) {
        $ids = array();
        foreach ($r['aggregations']['ids']['buckets'] as $bucket) {
          if ($bucket['key'] != $id) {
            $ids[$bucket['key']] = array();
          }
        }
        if (count($ids) > 0) {
          $subQuery = array(
            'size' => 20,
            'query' => array(
              'ids' => array(
                'values' => array_keys($ids)
              )
            )
          );
          $r = $this->getServerClient()->search($index, $subQuery, $this->isLegacy() ? $mapping : null);
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
      $query = array(
        'query' => array(
          'match_all' => array(
            'boost' => 1
          )
        )
      );
      $this->getServerClient()->search(static::RECO_INDEX_NAME, $query, $this->isLegacy() ? 'path' : null);
    }
    catch(ServerClientException $ex) {
      if($ex->getStatusCode() == 404) {
        //reco index does not exist
        $settings = [
          'number_of_shards' => $this->indexNumberOfShards,
          'number_of_replicas' => $this->indexNumberOfReplicas,
        ];
        $this->getServerClient()->createIndex(static::RECO_INDEX_NAME, $settings);
        $json = json_decode(file_get_contents(__DIR__ . '/../Resources/reco_structure.json'), TRUE);
        $this->getServerClient()->putMapping(static::RECO_INDEX_NAME, $this->isLegacy() ? 'path' : null, ['properties' => $json]);
      }
    }
    $document = array(
      'host' => $path['host'],
      'ids' => $path['ids'],
    );
    $r = $this->getServerClient()->index(static::RECO_INDEX_NAME, $document, $path['id'], $this->isLegacy() ? 'path' : null);
    $this->getServerClient()->flush();
    unset($document);
    return $r;
  }
}