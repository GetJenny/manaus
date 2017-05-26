package com.getjenny.manaus

class ObservedTokensOccurrencesMap(occurrences_map: Map[String, Int]) extends TokensOccurrences {
  val tokensN = occurrences_map.values.sum

  def getOccurrence(word: String): Int = {
    occurrences_map.getOrElse(word, 0)
  }

  def getTokenN(): Int = {
    tokensN
  }
}