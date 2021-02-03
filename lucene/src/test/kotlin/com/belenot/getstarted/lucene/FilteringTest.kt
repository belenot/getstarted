package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.BytesRef
import org.junit.After
import org.junit.Test
import java.io.File
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.Path

/*
    Difference between filter and query: filter doesn't do scoring.

    Lucene provides defaults filters:
    * TermRangeFilter
    * NumericRangeFilter
    * FieldCacheRangeFilter
    * QueryWrapperFilter
    * PrefixFilter
    * FieldCacheTermsFilter
    * FieldValueFilter
    * CachingWrapperFilter
 */
class FilteringTest {

    val directoryPath = Files.createTempDirectory("lucene")

    val lyricsFilenames = listOf(
        "Godzilla.txt",
        "Hurt.txt",
        "Smells Like Teen Spirit.txt",
        "Zombie.txt"
    )

    @Test fun `test`() {
        FSDirectory.open(directoryPath).use { directory ->
            val indexWriterConfig = IndexWriterConfig()
            val indexWriter = IndexWriter(directory, indexWriterConfig)
            val document = Document()
            val titleField = StringField("title", "", Field.Store.YES)
            val textField = TextField("text", "", Field.Store.YES)

            for (filename in lyricsFilenames) {
                val text = ClassLoader.getSystemResource(filename).readText()

                document.removeField("title")
                document.removeField("text")

                titleField.setStringValue(filename.replace("\\.txt", ""))
                textField.setStringValue(text)

                document.add(titleField)
                document.add(textField)

                indexWriter.addDocument(document)
            }
            indexWriter.commit()

            val indexReader = DirectoryReader.open(directory)
            val indexSearcher = IndexSearcher(indexReader)
            val query = TermQuery(Term("text", "hey"))

            val topDocs = indexSearcher.search(query, 10)
            for (scoreDoc in topDocs.scoreDocs) {
                val doc = indexReader.document(scoreDoc.doc)
                println(doc["title"])
            }

        }
    }

    @After fun destroy() {
        Files.walk(directoryPath)
            .sorted(Comparator.reverseOrder())
            .forEach { Files.delete(it) }
    }
}
