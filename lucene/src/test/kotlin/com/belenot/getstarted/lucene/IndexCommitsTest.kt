package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.*
import org.apache.lucene.store.FSDirectory
import org.hamcrest.MatcherAssert.*
import org.hamcrest.Matchers.*
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

class IndexCommitsTest {

    @Test fun `test rollback`() {
        val analyzer = StandardAnalyzer()
        val indexWriterConfig = IndexWriterConfig(analyzer)
        val indexDeletionPolicy = SnapshotDeletionPolicy(NoDeletionPolicy.INSTANCE)
        val fsDirectory = FSDirectory.open(randomDir())
        indexWriterConfig.setIndexDeletionPolicy(indexDeletionPolicy)
        val snapshots = with (IndexWriter(fsDirectory, indexWriterConfig)) {
            (0..2).map {
                val document = Document()
                addDocument(document)
                commit()
                indexDeletionPolicy.snapshot()
            }
        }.toList()
        val commits = DirectoryReader.listCommits(fsDirectory)
        commits.forEach {
            with (DirectoryReader.open(it)) {
                println(listOf(it.segmentCount, this.numDocs()))
            }
        }

        snapshots.forEach {
            with(DirectoryReader.open(it)) {
                println(listOf(it.segmentCount, this.numDocs()))
            }
        }

        assertThat("snapshots count", snapshots.size, equalTo(indexDeletionPolicy.snapshotCount))
        indexDeletionPolicy.release(snapshots.last())
        assertThat("snapshots count", snapshots.size, equalTo(indexDeletionPolicy.snapshotCount + 1))

    }

    private fun genUuidString() = UUID.randomUUID().toString()

    private fun randomDir(prefix: String = "/home/belenot/lucene") =
        Path.of("${prefix}/${genUuidString()}/").also {
            if (!Files.exists(it)) Files.createDirectory(it)
        }
}