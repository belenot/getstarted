package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.hamcrest.MatcherAssert.*
import org.hamcrest.Matchers.*
import org.junit.Test
import java.io.Reader
import java.io.StringReader

class MainTest {

    val luceneApp = LuceneApp()

    @Test fun addTest() {
        luceneApp.addText("lol")
    }

    @Test fun searchTest() {
        // Given
        luceneApp.addText("lol")
        // When
        val scoreDocs = luceneApp.searchText("lol")
        // Then
        assertThat("score size", scoreDocs.size, greaterThan(1))
        scoreDocs.forEach {
            val textValue = luceneApp.indexSearcher.doc(it.doc).get("text")
            assertThat("text value", textValue, equalTo("lol"))
        }
    }

    data class Attributes(val term: String, val start: Int, val end: Int, val positionIncrement: Int, val gender: GenderAttribute.Gender)

    @Test fun `obtaining token stream test`() {
        // Given
        val text = "Lucene is mainly used for information retrieval" +
                " and you can read more about it at lucene.apache.org"
        val reader: Reader = StringReader(text)
        val analyzer: Analyzer = StandardAnalyzer()
        val tokenStream: TokenStream = analyzer.tokenStream("text", reader)
        val attributes = attributesFromTokenStream(tokenStream)
        val words = text.split(" ").toSet()
        assertThat("words count", words.size, equalTo(attributes.size))
        text.split(" ").zip(attributes).forEach {
            assertThat("word ${it.first.toLowerCase()} in terms", it.first.toLowerCase(), equalTo(it.second.term))
            assertThat("position increment", it.second.positionIncrement, equalTo(1))
        }
        tokenStream.end()
        tokenStream.close()
        analyzer.close()
        println(attributes)
    }

    @Test fun `stop word filter`() {
        // Given
        val text = "This is a good morning"
        val reader = StringReader(text)
        val analyzer = StandardAnalyzer(StringReader("is\na\nthe"))
        val tokenStream = analyzer.tokenStream("text", reader)
        // When
        val attributes = attributesFromTokenStream(MyStopFilter(tokenStream).tokenStream)
        // Then
        val goodPosInc = attributes.filter{ it.term == "good"}.first().positionIncrement
        assertThat("'good' term postition increment", goodPosInc, equalTo(3))
    }

    @Test fun `gender analyzer`() {
        val text = "The film Mr and Mrs Smith"
        val reader = StringReader(text)
        val analyzer = CortesyTitleAnalyzer()
        val tokenStream = analyzer.tokenStream("text", reader)
        val attributes = attributesFromTokenStream(tokenStream)
        println(attributes)
        assertThat("Mr is male", attributes[2].gender, equalTo(GenderAttribute.Gender.MALE))
        assertThat("Mrs is female", attributes[4].gender, equalTo(GenderAttribute.Gender.FEMALE))
        assertThat("Smith is undefined", attributes[5].gender, equalTo(GenderAttribute.Gender.UNDEFINED))
    }

    private fun attributesFromTokenStream(tokenStream: TokenStream): List<Attributes> {
        val offsetAttribute = tokenStream.addAttribute(OffsetAttribute::class.java)
        val charTermAttribute = tokenStream.addAttribute(CharTermAttribute::class.java)
        val genderAttribute = tokenStream.addAttribute(GenderAttribute::class.java)
        val positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute::class.java)
        tokenStream.reset()

        val termOffsets = generateSequence {
            if (tokenStream.incrementToken())
                Attributes(
                    term = charTermAttribute.toString(),
                    start = offsetAttribute.startOffset(),
                    end = offsetAttribute.endOffset(),
                    positionIncrement = positionIncrementAttribute.positionIncrement,
                    gender = genderAttribute.gender
                )
            else null
        }.toList()
        return termOffsets
    }
}
