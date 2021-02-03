package com.belenot.getstarted.lucene

import org.apache.lucene.analysis.*
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.util.Attribute
import org.apache.lucene.util.AttributeImpl
import org.apache.lucene.util.AttributeReflector
import java.io.Reader
import java.nio.file.Path

fun main() {

}

class LuceneApp {
    val analyzer = StandardAnalyzer()
    val directory = MMapDirectory(Path.of("/tmp"))
    val indexWriterConfig = IndexWriterConfig(analyzer)
    val indexWriter = IndexWriter(directory, indexWriterConfig)
    val indexReader = DirectoryReader.open(directory)
    val indexSearcher = IndexSearcher(indexReader)

    fun addText(text: String) {
        val document = Document()
        document.add(TextField("text", text, Field.Store.YES))
        indexWriter.addDocument(document)
        indexWriter.commit()
    }

    fun searchText(text: String): Array<ScoreDoc> {
        val queryParser = QueryParser("text", analyzer)
        val query = queryParser.parse(text)
        val topDocs = indexSearcher.search(query, 10)
        return topDocs.scoreDocs
    }
}

class MyStopFilter(val tokenStream: TokenStream) : TokenFilter(tokenStream) {

    val stopWords = listOf("in", "is", "a", "the")

    val charTermAttribute = tokenStream.addAttribute(CharTermAttribute::class.java)
    val positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute::class.java)

    override fun incrementToken(): Boolean {
        var extraIncrement = 0
        var returnValue = false

        while(tokenStream.incrementToken()) {
            if (charTermAttribute.toString() in stopWords) {
                extraIncrement++
                continue
            }

            returnValue = true

            break
        }
        if (extraIncrement > 0) {
            positionIncrementAttribute.positionIncrement+=extraIncrement
        }
        return returnValue
    }
}

class CortesyTitleAnalyzer : Analyzer() {

    override fun createComponents(fieldName: String?): TokenStreamComponents {
        val tokenizer = StandardTokenizer()
        val filter = GenderFilter(CortesyTitleFilter(tokenizer))
        val tokenStreamComponents = TokenStreamComponents(tokenizer, filter)
        return tokenStreamComponents
    }
}

class CortesyTitleFilter(val tokenStream: TokenStream) : TokenFilter(tokenStream) {
    val termAttr = tokenStream.addAttribute(CharTermAttribute::class.java)
    val words = mapOf("Dr" to "doctor", "Mr" to "mister", "Mrs" to "misters")

    override fun incrementToken(): Boolean {
        if (!tokenStream.incrementToken()) return false

        if (termAttr.toString() in words.keys) {
            val short = termAttr.toString()
            termAttr.setEmpty().append(words[short].also{println("Append $it")})
        }
        return true
    }
}

class MyTokenizer : Tokenizer() {
    override fun incrementToken() = input.read().toChar() == ' '
}

interface GenderAttribute: Attribute {
    enum class Gender { MALE, FEMALE, UNDEFINED }

    var gender: Gender
}

class GenderAttributeImpl: AttributeImpl(), GenderAttribute {
    var _gender: GenderAttribute.Gender = GenderAttribute.Gender.UNDEFINED

    override var gender: GenderAttribute.Gender
        get() = _gender
        set(value) {_gender = value}

    override fun clear() {
        _gender = GenderAttribute.Gender.UNDEFINED
    }

    override fun reflectWith(reflector: AttributeReflector?) {
        TODO("Not yet implemented")
    }

    override fun copyTo(target: AttributeImpl?) {
        if (target is GenderAttribute) {
            target.gender = gender
        }
    }
}

class GenderFilter(val tokenStream: TokenStream) : TokenFilter(tokenStream) {

    val genderAttr = addAttribute(GenderAttribute::class.java)
    val charTermAttr = getAttribute(CharTermAttribute::class.java)


    override fun incrementToken() =
        if (!input.incrementToken())
            false
        else
            true.also { genderAttr.gender = determineGender(charTermAttr.toString()) }

    fun determineGender(term: String) =
        if (term.toLowerCase() in setOf("mr", "mister"))
            GenderAttribute.Gender.MALE
        else if (term.toLowerCase() in setOf("mrs", "misters"))
            GenderAttribute.Gender.FEMALE
        else
            GenderAttribute.Gender.UNDEFINED

}
