package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.*
import org.apache.lucene.store.FSDirectory
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path

class SearcherManagerTest {
    val path = Files.createTempDirectory("lucene")
    val fsDirectory = FSDirectory.open(path)
    val names = listOf("Jack", "John", "Jane")

    @Test fun `create searcher manager`() {

        IndexWriter(fsDirectory, IndexWriterConfig()).use { indexWriter ->
            val searcherManager = SearcherManager(indexWriter, true, true, SearcherFactory())
            val document = Document()
            val textField = StringField("name", "", Field.Store.YES)
            for (name in names) {
                document.removeField("name")
                textField.setStringValue(name)
                document.add(textField)
                indexWriter.addDocument(document)
            }
            // Try to search without commiting
            searcherManager.maybeRefresh()
            val indexSearcher = searcherManager.acquire()
//            indexWriter.commit()
//            val indexReader = DirectoryReader.open(indexWriter.directory)

//            assertThat("First doc with field name=John", indexReader.document(0)["name"], equalTo(names[0]))
//            val indexSearcher = IndexSearcher(indexReader)
            val topDocs = indexSearcher.search(WildcardQuery(Term("name", "John")), 10)
            assertThat("top docs size", topDocs.scoreDocs.size, equalTo(1))
        }
    }

    @Test fun `Generational indexing with TrackingIndexWriter`() {
        // Given
        val indexWriter = IndexWriter(fsDirectory, IndexWriterConfig())
        val searcherManager = SearcherManager(indexWriter, true, true, SearcherFactory())
        val controlledRealTimeReopenThread = ControlledRealTimeReopenThread(indexWriter, searcherManager, 5.0, 0.005)
        controlledRealTimeReopenThread.start()
        // When
        val indexGeneration1 = indexWriter.addDocument(Document().apply{ add(TextField("name", "kek", Field.Store.YES))})
        indexWriter.addDocument(Document().apply{ add(TextField("name", "kek", Field.Store.YES))})
        indexWriter.addDocument(Document().apply{ add(TextField("name", "kek", Field.Store.YES))})
        controlledRealTimeReopenThread.waitForGeneration(indexGeneration1)
        // Then
        assert(searcherManager.acquire().search(WildcardQuery(Term("name", "kek")), 10).scoreDocs.size == 3)
        // When
        val indexGeneration2 = indexWriter.addDocument(Document().apply{ add(TextField("name", "kek", Field.Store.YES))})
        controlledRealTimeReopenThread.waitForGeneration(indexGeneration1)
        // Then
        assert(searcherManager.acquire().search(WildcardQuery(Term("name", "kek")), 10).scoreDocs.size == 3)
        // When
        controlledRealTimeReopenThread.waitForGeneration(indexGeneration2)
        // Then
        assert(searcherManager.acquire().search(WildcardQuery(Term("name", "kek")), 10).scoreDocs.size.also{println(it)} == 4)
        // When
        controlledRealTimeReopenThread.waitForGeneration(indexGeneration1)
        // Then
        assert(searcherManager.acquire().search(WildcardQuery(Term("name", "kek")), 10).scoreDocs.size.also{println(it)} == 4)
    }
}