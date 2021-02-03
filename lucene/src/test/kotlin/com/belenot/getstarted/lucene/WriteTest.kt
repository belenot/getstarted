package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.*
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.util.QueryBuilder
import org.hamcrest.MatcherAssert.*
import org.hamcrest.Matchers.*
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path

class WriteTest {

    val dirPath = Path.of("/home/belenot/lucene")

    @Test fun `create WriteIndex`() {
        if (! Files.exists(dirPath)) Files.createDirectory(dirPath)

        val fsDirectory = FSDirectory.open(dirPath)
        val analyzer = StandardAnalyzer()

        val indexWriterConfig = IndexWriterConfig(analyzer).apply {
            openMode = IndexWriterConfig.OpenMode.CREATE
            ramBufferSizeMB = 64.0
            maxBufferedDocs = 4000
        }

        val indexWriter = IndexWriter(fsDirectory, indexWriterConfig)

        val text = "Lucene is an Information Retrieval library written in Java."
        val document = Document()
        document.add(StringField("telephone_number", "89660411827", Field.Store.YES))
        document.add(StringField("area_code", "0484", Field.Store.YES))
        document.add(TextField("text", text, Field.Store.YES))

        indexWriter.addDocument(document)
        indexWriter.commit()

        val indexReader = DirectoryReader.open(fsDirectory)
        val indexSearcher = IndexSearcher(indexReader)
        val query = QueryBuilder(analyzer).createBooleanQuery("area_code", "0484")
        val topDocs = indexSearcher.search(query, 10)
        assertThat("top docs size", topDocs.scoreDocs.size, greaterThanOrEqualTo(1))
        println(topDocs.scoreDocs.map{it.doc})

        val areaCode = indexReader.document(topDocs.scoreDocs[0].doc)["area_code"]
        assertThat("area code", areaCode, equalTo("0484"))
        println(areaCode)
    }

    @Test fun `reusing documents`() {
        // Given
        val dirPath = Files.createTempDirectory("lucene")
        val fsDirectory = MMapDirectory(dirPath)
        val analyzer = StandardAnalyzer()
        val indexWriterConfig = IndexWriterConfig(analyzer)
        val indexWriter = IndexWriter(fsDirectory, indexWriterConfig)
        // When
        val document = Document()
        val stringField = StringField("name", "", Field.Store.YES)
        val names = listOf("John", "Jack", "Jane").map {
            stringField.setStringValue(it)
            document.removeField("name")
            document.add(stringField)
            indexWriter.addDocument(document)
            it
        }
        indexWriter.commit()
        // Then
        val indexReader = DirectoryReader.open(fsDirectory)

        for (index in 0..2) {
            val d = indexReader.document(index)
            assertThat("name in index are the same as in list", d["name"], equalTo(names[index]))
            println(d["name"])
        }
    }


}