<?php

namespace AdimeoDataSuite\Model;


class Autopromote extends PersistentObject
{
  /** @var  string */
  private $id;
  /** @var  string */
  private $name;
  /** @var  string */
  private $url;
  /** @var  string */
  private $image;
  /** @var  string */
  private $body;
  /** @var  string */
  private $keywords;
  /** @var  string */
  private $index;
  /** @var  string */
  private $analyzer;

  /**
   * Autopromote constructor.
   * @param string $id
   * @param string $title
   * @param string $url
   * @param string $image
   * @param string $body
   * @param string $keywords
   * @param string $index
   * @param string $analyzer
   */
  public function __construct($id, $name, $url, $image, $body, $keywords, $index, $analyzer)
  {
    $this->id = $id;
    $this->name = $name;
    $this->url = $url;
    $this->image = $image;
    $this->body = $body;
    $this->keywords = $keywords;
    $this->index = $index;
    $this->analyzer = $analyzer;
  }

  /**
   * @return string
   */
  public function getId()
  {
    return $this->id;
  }

  /**
   * @param string $id
   */
  public function setId($id)
  {
    $this->id = $id;
  }

  /**
   * @return string
   */
  public function getName()
  {
    return $this->name;
  }

  /**
   * @param string $title
   */
  public function setName($name)
  {
    $this->name = $name;
  }

  /**
   * @return string
   */
  public function getUrl()
  {
    return $this->url;
  }

  /**
   * @param string $url
   */
  public function setUrl($url)
  {
    $this->url = $url;
  }

  /**
   * @return string
   */
  public function getImage()
  {
    return $this->image;
  }

  /**
   * @param string $image
   */
  public function setImage($image)
  {
    $this->image = $image;
  }

  /**
   * @return string
   */
  public function getBody()
  {
    return $this->body;
  }

  /**
   * @param string $body
   */
  public function setBody($body)
  {
    $this->body = $body;
  }

  /**
   * @return string
   */
  public function getKeywords()
  {
    return $this->keywords;
  }

  /**
   * @param string $keywords
   */
  public function setKeywords($keywords)
  {
    $this->keywords = $keywords;
  }

  /**
   * @return string
   */
  public function getIndex()
  {
    return $this->index;
  }

  /**
   * @param string $index
   */
  public function setIndex($index)
  {
    $this->index = $index;
  }

  /**
   * @return string
   */
  public function getAnalyzer()
  {
    return $this->analyzer;
  }

  /**
   * @param string $analyzer
   */
  public function setAnalyzer($analyzer)
  {
    $this->analyzer = $analyzer;
  }

  function getType()
  {
    return 'autopromote';
  }

  public function getTags()
  {
    return array(
      'index_name=' . $this->getIndex()
    );
  }

}