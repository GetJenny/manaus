package com.getjenny.manaus.commands

/**
  * Created by angelo on 03/07/17.
  */

import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.rest.RestStatus

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

case class UpdateDocumentResult(index: String,
                                dtype: String,
                                id: String,
                                version: Long,
                                created: Boolean
                               )

case class KBDocumentUpdate(
                             question_scored_terms: Option[List[(String, Double)]] = None: Option[List[(String, Double)]] ,
                             answer_scored_terms: Option[List[(String, Double)]] = None: Option[List[(String, Double)]]
                           )

case class ElasticClientKB(indexSuffix: String, queryMinThreshold: Double,
                           indexName: String, clusterName: String,
                           ignoreClusterName: Boolean, indexLanguage: String,
                           hostMap: Map[String, Int], keystorePath: String,
                           keystorePassword: String, hostProto: String,
                           certFormat: String, disableHostValidation: Boolean,
                           elasticsearchAuthentication: String
                          ) extends ElasticClient {

  def updateDocument(id: String, document: KBDocumentUpdate, elasticClient: ElasticClient):
                                                            Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()
    document.question_scored_terms match {
      case Some(t) =>
        val array = builder.startArray("question_scored_terms")
        t.foreach{case(term, score) =>
          array.startObject().field("term", term)
            .field("score", score).endObject()
        }
        array.endArray()
      case None => ;
    }

    document.answer_scored_terms match {
      case Some(t) =>
        val array = builder.startArray("answer_scored_terms")
        t.foreach{case(term, score)=>
          array.startObject().field("term", term)
            .field("score", score).endObject()
        }
        array.endArray()
      case None => ;
    }
    builder.endObject()

    val client: RestHighLevelClient = elasticClient.httpClient

    val request: UpdateRequest = new UpdateRequest()
      .index(elasticClient.indexName)
      .id(id)
      .doc(builder)

    val response: UpdateResponse = client.update(request, RequestOptions.DEFAULT)

    val doc_result: UpdateDocumentResult = UpdateDocumentResult(index = response.getIndex,
      dtype = response.getType,
      id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    Option {doc_result}
  }

}

