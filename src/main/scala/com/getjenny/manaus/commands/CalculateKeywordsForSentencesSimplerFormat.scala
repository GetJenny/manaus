package com.getjenny.manaus.commands

import com.getjenny.manaus.util._
import com.getjenny.manaus._
import breeze.io.{CSVReader, CSVWriter}
import java.io.{File, FileWriter, FileReader}
import scopt.OptionParser
import scala.io.Source
import scala.collection.SeqView

object CalculateKeywordsForSentencesSimplerFormat {

  private case class Params(
    raw_conversations: String = "data/conversations.txt",
    separator: Char = ';',
    word_frequencies: String = "statistics_data/english/word_frequency.tsv",
    minWordsPerSentence: Int = 10,
    pruneTermsThreshold: Int = 100000,
    misspell_max_occurrence: Int = 5,
    output_file: String = "",
    active_potential_decay: Int = 10,
    total_info: Boolean = false,
    active_potential: Boolean = true,
    compute_bigrams: Boolean = true
  )

  def doKeywordExtraction(params: Params): Unit = {
    // Load the prior occurrences

    val cmd_utils = CommandsUtils

    val minWordsPerSentence = params.minWordsPerSentence
    val pruneTermsThreshold = params.pruneTermsThreshold
    val misspell_max_occurrence = params.misspell_max_occurrence
    val priorOccurrences = cmd_utils.readPriorOccurrencesMap(params.word_frequencies)
    val active_potential_decay = params.active_potential_decay
    val active_potential = params.active_potential
    val total_info = params.total_info
    val compute_bigrams = params.compute_bigrams

    println("INFO: getting sentences and observedOccurrences: " + params)
    val (sentences, observedOccurrences) =
      cmd_utils.buildObservedOccurrencesMapFromConversationsFormat3(params.raw_conversations, params.separator)

    val bags =  cmd_utils.extractKeywords(sentences = sentences, observedOccurrences = observedOccurrences,
                  minWordsPerSentence = minWordsPerSentence,
                  pruneTermsThreshold = pruneTermsThreshold , misspell_max_occurrence = misspell_max_occurrence,
                  priorOccurrences = priorOccurrences, active_potential_decay = active_potential_decay,
                  active_potential = active_potential, total_info = total_info)

    println("INFO: merging sentences with bags")

    val out_keywords = sentences.zip(bags).map(item => {
      val sentence = item._1
      val bag = item._2
      val keywords = bag._2.toSeq.sortBy(- _._2).map(x => x._1 + "|" + x._2.toString).mkString(" ")
      IndexedSeq(sentence._1, keywords)
    })

    println("INFO: results serialization on file")

    val output_file = new File(params.output_file)
    val file_writer = new FileWriter(output_file)

    //sentence, type, conv_id, sentence_id
    CSVWriter.write(output=file_writer,
      mat=out_keywords,
      separator=';',
      quote='"',
      escape='\\')

    println("INFO: keywords calculation completed")

    if( compute_bigrams ) {
      println("INFO: start with Bigrams")
      val g = Bags(bags.toList.map(x => (x._1, x._2.map(_._1).toSet)))
      println("Bigrams:\n" + g.binomialSignificativeBigrams.filter(_._2 > 20).mkString("\n"))
    }

  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("KeywordExtractionSample") {
      head("extract the most relevant keywords from text.")
      help("help").text("prints this usage text")
      opt[String]("raw_conversations").required()
        .text(s"the file with raw conversation, a conversation per line with interactions separated by --separator" +
          s"  default: ${defaultParams.raw_conversations}")
        .action((x, c) => c.copy(raw_conversations = x))
      opt[String]("separator").required()
        .text(s"fields' separator in raw conversation" +
          s"  default: ${defaultParams.separator}")
        .action((x, c) => c.copy(separator = x.charAt(0)))
      opt[String]("word_frequencies").required()
        .text(s"the file with word frequencies" +
          s"  default: ${defaultParams.word_frequencies}")
        .action((x, c) => c.copy(word_frequencies = x))
      opt[String]("output_file").required()
        .text(s"the output file")
        .action((x, c) => c.copy(output_file = x))
      opt[Int]("min_words_in_sentence")
        .text(s"discard the sentences with less that N words" +
          s"  default: ${defaultParams.minWordsPerSentence}")
        .action((x, c) => c.copy(minWordsPerSentence = x))
      opt[Int]("prune_sentence_threshold")
        .text(s"threshold on the number of terms for trigger pruning" +
          s"  default: ${defaultParams.pruneTermsThreshold}")
        .action((x, c) => c.copy(pruneTermsThreshold = x))
      opt[Int]("mispell_max_occurrence")
        .text(s"given a big enough sample, min freq beyond what we consider the token a misspell" +
          s"  default: ${defaultParams.misspell_max_occurrence}")
        .action((x, c) => c.copy(misspell_max_occurrence = x))
      opt[Int]("active_potential_decay")
        .text(s"introduce a penalty on active potential for words which does not occur enough" +
          s"  default: ${defaultParams.active_potential_decay}")
        .action((x, c) => c.copy(active_potential_decay = x))
      opt[Boolean]("total_info")
        .text(s"normalize the information by total informations" +
          s"  default: ${defaultParams.total_info}")
        .action((x, c) => c.copy(total_info = x))
      opt[Boolean]("active_potential")
        .text(s"weight bags with active potential" +
          s"  default: ${defaultParams.active_potential}")
        .action((x, c) => c.copy(active_potential = x))
      opt[Boolean]("compute_bigrams")
        .text(s"compute bigrams" +
          s"  default: ${defaultParams.compute_bigrams}")
        .action((x, c) => c.copy(compute_bigrams = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doKeywordExtraction(params)
      case _ =>
        sys.exit(1)
    }
  }

}