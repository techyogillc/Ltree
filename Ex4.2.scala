object SentenceRank {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Sentence Rank")
        val sc = new SparkContext(conf)

        //INFO 1
        //
        // local and HDFS file URLs
        //
        //val in = "hdfs://localhost:9000/user/spark/data/legislators/sou-2012"
        //val out = "hdfs://localhost:9000/user/spark/ex-4-2-results"
        //
        val in = "file:///home/spark/crs1262/data/legislators/sou-2012"
        val out = "file:///home/spark/crs1262/exercises/Solution-4.2/results"

        // produce distinct (index, clean_sentence) tuples
        //
        // NOTE 1: letters, numbers, commas, spaces and single quotes are not filtered out of the sentences
        //      2: textFile() uses a Hadoop TextInputFormat which by default splits text data using "\n"
        //         override this default to use "." allowing it to split by sentence
        //
        sc.hadoopConfiguration.set("textinputformat.record.delimiter", ".")
        val sentences = sc.textFile(in)
                          .map(_.replaceAll("[^a-zA-Z0-9', ]", "").trim)
                          .filter(_.length > 0)
                          .distinct()                // remove any duplicate sentences
                          
        //TODO 1                  
                          .zipWithIndex()            // create a (sentence, sentence-index) tuple
                          .map(_.swap)               // convert to (sentence-index, sentence)

        //TODO 2
        //
        // keep this RDD in memory as it will be used in 3 different places
        //
        sentences.cache()
        
        // produce (word, occurrence_count) tuples, computed across all sentences
        //
        // NOTE: commas are removed from the ends of words
        // FIXME! assumes that they don't occur within...
        //
        val wordCounts = sentences.flatMap(_._2.split(" ").map(_.filter(_ != ',')))
                                  .map((_, 1))
        //TODO 3
                                  .reduceByKey(_ + _)

        //INFO 2
        //
        // produce (word, sentence_index) tuples for each sentence
        //
        val sentenceSplit = sentences.flatMap( sentence => {
            val tuples = MutableList[(String, Long)]()
            sentence._2.split(" ")
              .map( _.filter(_ != ',' ))
        //    .filter( _.length > 4 )                       // optionally remove short words, as they can skew the results
              .map( w => tuples += ((w, sentence._1)) )
              //.foreach( w => tuples += ((w, sentence._1)) ) // an alternative "less functional" approach!
            tuples
        })

        //TODO 4
        //
        // produce (sentence_index, CompactBuffer(word_occurrence_count, word_occurrence_count, ...)) tuples
        //
        // NOTE 1: performs a join on (word, sentence_index) and (word, occurrence_count)
        //         this produces (word, (sentence_index, word_occurrence_count))
        //      2: this is then re-mapped to (sentence_index, word_occurrence_count)
        //      3: the above tuples are now grouped by sentence_index
        //         i.e. produces tuples that look like (sentence_index, (word_occurrence_count_1, word_occurrence_count_2, ...))
        //
        val  sentenceWordOccurrences = sentenceSplit.join(wordCounts)
                                                    .map(t => (t._2._1, t._2._2))
                                                    .groupByKey()

        //INFO 3
        //
        // produce (sentence_index, average_of_the_associated_word_occurrences)
        //
        // NOTE 1: map() is using pattern matching to assign variables to the contained tuples
        //         the same technique is applied to the foldLeft()
        //      2: uses foldLeft() to count and sum up the word occurrences
        //      3: here, the initial fold value is a tuple set to (0, 0) and referenced as (sum, count) in the lambda
        //      4: wcBuffer is the collection of word occurrences associated with a sentence_index
        //         in the foldLeft(), the lambda refers to the running (sum, count) and
        //         a particular word occurrence value taken from wcBuffer
        //
        val sentenceRank =  sentenceWordOccurrences.map {
            case(sentenceIndex, wcBuffer) => {
                val (sum, count) = wcBuffer.foldLeft((0, 0)) {
                    case((sum, count), wordCount) => (sum + wordCount, count + 1)
                }

                (sentenceIndex, (100 * sum.toDouble / count).round)
            }
        }

        //TODO 5
        //
        // produce (rank, sentence) tuples
        //
        // NOTE 1: performs a join on (sentence_index, rank) and (sentence_index, sentence)
        //         this produces (sentence_index, (rank, sentence))
        //      2: map out the sentence_index as it's of no interest
        //         produces (rank, sentence)
        //      3: for readability add a "." back on to the end of each sentence
        //         these were lost as they were used as the "split string" on the input text file
        //      4: sort the results by rank
        //
        val results = sentenceRank.join(sentences)
                                  .map(tuple => (tuple._2._1, tuple._2._2 + "."))
                                  .sortByKey(ascending = false)

        // INFO 4
        //
        // save the results and shutdown the SparkContext
        //
        results.saveAsTextFile(out)

        sc.stop()
    }
}

