package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.FieldType
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.junit.Test

import org.hamcrest.Matchers.*
import org.hamcrest.MatcherAssert.*
import java.nio.file.Files

class IndexReaderTest() {

    val path = Files.createTempDirectory("lucene")


    @Test fun `iterate atomic readers`() {
        // Given
        val fsDirectory = FSDirectory.open(path)
        val indexReader = DirectoryReader.open(fsDirectory)
        indexReader.leaves().forEach {
            it
        }
    }

    @Test fun `retrieve terms stats`() {
        // Given
        val analyzer = StandardAnalyzer()
        val indexWriterConfig = IndexWriterConfig(analyzer)
        val fsDirectory = FSDirectory.open(path)
        val indexWriter = IndexWriter(fsDirectory, indexWriterConfig)

        val fieldType = FieldType().apply {
            setStored(true)
            setTokenized(true)
            setStoreTermVectors(true)
            setStoreTermVectorOffsets(true)
            setStoreTermVectorPositions(true)
        }

        val document = Document()
        val field = Field("text", "", fieldType)
        val contents = listOf(
            "Humpty Dumpty sat on a wall",
            "All the king's horses and all the king's men",
            "Couldn't put Humpty together again"
        )

        for (content in contents) {
            field.setStringValue(content)
            document.removeField("text")
            document.add(field)
            indexWriter.addDocument(document)
        }
        indexWriter.commit()


    }
}