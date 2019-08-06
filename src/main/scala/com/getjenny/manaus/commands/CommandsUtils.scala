package com.getjenny.manaus.commands

import java.io.{File, FileReader}
import java.util.Collections

import breeze.io.CSVReader
import com.getjenny.manaus._
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.search._
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{MatchAllQueryBuilder, QueryBuilders}
import org.elasticsearch.script.{Script, _}
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.io.Source


object CommandsUtils extends LazyLogging {

  def readPriorOccurrencesMap(word_frequencies: String,
                              wordColumn: Int = 1, occurrenceColumn: Int = 2): TokensOccurrences = {
    val priorOccurrencesMap: Map[String, Int] = Source.fromFile(word_frequencies).getLines
      .map(line => {
        val split_line = line.split("\t")
        split_line(wordColumn).toLowerCase -> line.split("\t")(occurrenceColumn).toInt
      }).toMap.withDefaultValue(0)

    val priorOccurrences = new PriorTokensOccurrencesMap(priorOccurrencesMap)
    priorOccurrences
  }

  def getDataFromCSV(conversations_file: String, separator: Char=';'): Stream[IndexedSeq[String]] = {
    val file = new File(conversations_file)
    val file_reader = new FileReader(file)
    val file_entries = CSVReader.read(input=file_reader, separator=separator,
      quote='"', escape='\\', skipLines=0)
    file_entries.toStream
  }

  def buildObservedOccurrencesMapFromConversationsFormat1(conversations_file: String, separator: Char=';') = {
    // list of tokenized sentences grouped by conversation
    // (sentence, tokenized_sentence, type, conv_id, sentence_id)
    def sentences = getDataFromCSV(conversations_file, separator).map(line => {
      val tokenized_sentence = line(0).split(" ").toList.filter(_ != "").map(w => w.toLowerCase)
      (line(0), tokenized_sentence, line(1), line(2), line(3))
    })

    logger.info("calculating observedOcurrencesMap")
    val observedOccurrencesMap = sentences.flatMap(line => line._2)
      .foldLeft(Map.empty[String, Int]){
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }

    val observedOccurrences = new ObservedTokensOccurrencesMap(observedOccurrencesMap)

    (sentences, observedOccurrences)
  }

  def buildObservedOccurrencesMapFromConversationsFormat2(conversations_file: String, separator: Char=';') = {
    // list of tokenized sentences grouped by conversation
    // (sentence, tokenized_sentence, type, conv_id, sentence_id)
    def sentences = getDataFromCSV(conversations_file, separator).map(line => {
      val tokenized_sentence = line(0).split(" ").toList.filter(_ != "").map(w => w.toLowerCase)
      (line(1), tokenized_sentence)
    })

    logger.info("calculating observedOcurrencesMap")
    val observedOccurrencesMap = sentences.flatMap(line => line._2)
      .foldLeft(Map.empty[String, Int]){
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }

    val observedOccurrences = new ObservedTokensOccurrencesMap(observedOccurrencesMap)

    (sentences, observedOccurrences)
  }

  def buildObservedOccurrencesMapFromConversationsFormat3(conversations_file: String, separator: Char) = {

    // list of tokenized sentences grouped by conversation
    // (sentence, tokenized_sentence, type, conv_id, sentence_id)
    def sentences = getDataFromCSV(conversations_file, separator).map(line => {
      val tokenized_sentence = line(1).replaceAll("[^A-Za-z]+", " ").split("\\s+").toList.map(w => w.toLowerCase)
      (line(0), tokenized_sentence)
    })

    logger.info("calculating observedOcurrencesMap in Format3: file " + conversations_file + " and separator " + separator)
    val observedOccurrencesMap = sentences.flatMap(line => line._2)
      .foldLeft(Map.empty[String, Int]){
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }

    val observedOccurrences = new ObservedTokensOccurrencesMap(observedOccurrencesMap)

    (sentences, observedOccurrences)
  }

  def get_indices(elastic_client : ElasticClient): List[String] = {
    val clusterHealthReq = new ClusterHealthRequest()
    clusterHealthReq.level(ClusterHealthRequest.Level.INDICES)
    val clusterHealthRes = elastic_client.httpClient.cluster().health(clusterHealthReq, RequestOptions.DEFAULT)
    clusterHealthRes.getIndices.asScala.map { case (k, _) => k}.toList
  }

  def search(elastic_client : ElasticClient):
  Stream[(String, String)] = {

    elastic_client.openHttp()
    val client: RestHighLevelClient = elastic_client.httpClient
    val qb: MatchAllQueryBuilder = QueryBuilders.matchAllQuery()

    val sourceBuilder = new SearchSourceBuilder()
      .query(qb)
      .size(10000)

    val searchRequest: SearchRequest = new SearchRequest(elastic_client.indexName)
      .source(sourceBuilder)
      .scroll(new TimeValue(60000))

    var scrollResp: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val documents: Stream[(String, String)] = Stream.continually({
      val hits = scrollResp.getHits.getHits
      val scrollId = scrollResp.getScrollId
      val scrollRequest = new SearchScrollRequest(scrollId)
          .scroll(new TimeValue(60000))
      scrollResp = client.scroll(scrollRequest, RequestOptions.DEFAULT)
      hits
    }).takeWhile(_.length != 0).flatten.map(hit => {
      val id = hit.getId
      val source : Map[String, Any] = hit.getSourceAsMap.asScala.toMap
      val question : String = source.get("question") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }
      (question, id)
    })

    documents
  }

  def searchAndGetTokens(field_name: String, elastic_client: ElasticClient): Stream[(List[String], String)] = {

    elastic_client.openHttp()
    val client: RestHighLevelClient = elastic_client.httpClient
    val qb: MatchAllQueryBuilder = QueryBuilders.matchAllQuery()

    val script_text = "doc[\"" + field_name + "\"].values"

    val script: Script = new Script(
      ScriptType.INLINE,
      "painless",
      script_text,
      Collections.emptyMap())

    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
      .query(qb)
      .scriptField("analyzed_tokens", script)
      .size(100)

    val searchRequest: SearchRequest = new SearchRequest(elastic_client.indexName)
      .source(sourceBuilder)
      .scroll(new TimeValue(60000))

    var scrollResp: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val documents: Stream[(List[String], String)] = Stream.continually({
      val hits = scrollResp.getHits.getHits
      val scrollId = scrollResp.getScrollId
      val scrollRequest = new SearchScrollRequest(scrollId)
          .scroll(new TimeValue(60000))
      scrollResp = client.scroll(scrollRequest, RequestOptions.DEFAULT)
      hits
    }).takeWhile(_.length != 0).flatten.map(hit => {
      val id = hit.getId
      val analyzed_tokens = hit.getFields.get("analyzed_tokens").asScala.map(x => {
        val token = x.asInstanceOf[String]
        token
      })

      (analyzed_tokens.toList, id)
    })

    documents
  }

  def extractKeywords(sentences: Stream[(String, List[String])],
                      observedOccurrences: ObservedTokensOccurrencesMap,
                      minWordsPerSentence: Int, pruneTermsThreshold: Int, misspell_max_occurrence: Int,
                      priorOccurrences: TokensOccurrences, active_potential_decay: Int,
                      total_info: Boolean,
                      active_potential: Boolean): Stream[(List[String], Map[String, Double])] = {

    logger.info("extract keywords")
    val keywordsExtraction = new KeywordsExtraction(priorOccurrences=priorOccurrences,
      observedOccurrences=observedOccurrences)

    logger.info("extract informativeWords")
    /* Informative words */
    val rawBagOfKeywordsInfo: Stream[List[(String, Double)]] = sentences.map(sentence => {
      val informativeK = keywordsExtraction.extractInformativeWords(sentence = sentence._2,
        pruneSentence = pruneTermsThreshold, minWordsPerSentence = minWordsPerSentence,
        totalInformationNorm = total_info)
      informativeK
    })

    logger.info("calculating active potentials Map")
    /* Map(keyword -> active potential) */
    val activePotentialKeywordsMap = keywordsExtraction.getWordsActivePotentialMap(rawBagOfKeywordsInfo,
      active_potential_decay)

    logger.info("getting informative words for sentences")
    val informativeKeywords: Stream[(List[String], List[(String, Double)])] =
      sentences.zip(rawBagOfKeywordsInfo).map(sentence => {
        (sentence._1._2, sentence._2)
      })

    logger.info("calculating bags")
    // list of the final keywords
    val bags: Stream[(List[String], Map[String, Double])] =
      if(active_potential) {
        keywordsExtraction.extractBagsActive(activePotentialKeywordsMap = activePotentialKeywordsMap,
          informativeKeywords = informativeKeywords, misspellMaxOccurrence = misspell_max_occurrence)
      } else {
        keywordsExtraction.extractBagsNoActive(informativeKeywords = informativeKeywords,
          misspellMaxOccurrence = misspell_max_occurrence)
      }
    bags
  }

}
