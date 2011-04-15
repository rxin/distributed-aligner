package edu.berkeley.nlp.mt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.berkeley.nlp.io.IOUtils;
import edu.berkeley.nlp.mt.SentencePairReader.PairDepot;
import edu.berkeley.nlp.mt.SentencePairReader.PairIterator;
import edu.berkeley.nlp.util.Filters;
import edu.berkeley.nlp.util.Pair;

/**
 * A holder for a pair of sentences, each a list of strings. Sentences in the
 * test sets have integer IDs, as well, which are used to retreive the gold
 * standard alignments for those sentences.
 */
public class SentencePair
{

	public static final String ENGLISH_EXTENSION = "en";

	public static final String FRENCH_EXTENSION = "fr";

	public static Iterable<SentencePair> readSentencePairs(String path, int maxSentencePairs) {
		SentencePairReader sentencePairReader = new SentencePairReader();
		sentencePairReader.setForeignExtension(FRENCH_EXTENSION);
		sentencePairReader.setEnglishExtension(ENGLISH_EXTENSION);
		PairDepot sentencePairsIteratorFromFile = sentencePairReader.pairDepotFromSource(path, maxSentencePairs);
		return sentencePairsIteratorFromFile;

	}

	public SentencePair getReversedCopy() {
		return new SentencePair(this.getSentenceID(), this.getSourceFile(), this.getFrenchWords(), this.getEnglishWords());
	}

	int sentenceID;

	String sourceFile;

	public List<String> englishWords;

	public List<String> frenchWords;

	public int getSentenceID() {
		return sentenceID;
	}

	public String getSourceFile() {
		return sourceFile;
	}

	public List<String> getEnglishWords() {
		return englishWords;
	}

	public List<String> getFrenchWords() {
		return frenchWords;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int englishPosition = 0; englishPosition < englishWords.size(); englishPosition++) {
			String englishWord = englishWords.get(englishPosition);
			sb.append(englishPosition);
			sb.append(":");
			sb.append(englishWord);
			sb.append(" ");
		}
		sb.append("\n");
		for (int frenchPosition = 0; frenchPosition < frenchWords.size(); frenchPosition++) {
			String frenchWord = frenchWords.get(frenchPosition);
			sb.append(frenchPosition);
			sb.append(":");
			sb.append(frenchWord);
			sb.append(" ");
		}
		sb.append("\n");
		return sb.toString();
	}

	public SentencePair(int sentenceID, String sourceFile, List<String> englishWords, List<String> frenchWords) {
		this.sentenceID = sentenceID;
		this.sourceFile = sourceFile;
		this.englishWords = englishWords;
		this.frenchWords = frenchWords;
	}
}

