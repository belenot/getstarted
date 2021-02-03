package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.*
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.BytesRef
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import java.nio.file.Files

class IndexSearcherTest {

    val lines = listOf(
        "Load up on guns",
        "And bring your friends",
        "It's fine to loose",
        "And to pretend"
    )


    @Test fun `test searcher`() {
        FSDirectory.open(Files.createTempDirectory("lucene")).use { fsDirectory ->
            // Given
            val indexWriter = IndexWriter(
                fsDirectory,
                IndexWriterConfig()
            )
            val document = Document()

            //val lineField = TextField("line", "", Field.Store.YES)
            val sortedLineField = SortedDocValuesField("line", BytesRef(""))
            val lineField = TextField("line", "", Field.Store.YES)

            for (line in lines) {
                lineField.setStringValue(line)
                sortedLineField.setBytesValue(BytesRef(line))
                document.removeField("line")
                document.removeField("line")
                document.add(lineField)
                document.add(sortedLineField)
                indexWriter.addDocument(document)
            }
            indexWriter.commit()
            val indexReader = DirectoryReader.open(fsDirectory)
            val indexSearcher = IndexSearcher(indexReader)

            // When
//            val query = WildcardQuery(Term("line", "*"))
            val query = QueryParser("line", StandardAnalyzer()).parse("guns")
            val sortField = SortField("line", SortField.Type.STRING)

//            val sort = Sort(sortField)
            val topDocs = indexSearcher.search(query, 10)
            val secondTopDocs = indexSearcher.searchAfter(topDocs.scoreDocs.last(), query, 10)
            assertThat("second top docs count", secondTopDocs.scoreDocs.size, equalTo(0))
            // Then
            for (scoreDoc in topDocs.scoreDocs) {
                val id = scoreDoc.doc
                println("" + scoreDoc.score.toString() + " " + indexReader.document(id)["line"])
            }
            assertThat("Number of docs", topDocs.scoreDocs.size, equalTo(1))
        }
    }
}
