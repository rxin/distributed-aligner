package edu.berkeley.nlp.mt;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.berkeley.nlp.io.IOUtils;
import edu.berkeley.nlp.io.SuffixFilter;
import edu.berkeley.nlp.mt.decoder.Logger;
import edu.berkeley.nlp.util.CollectionUtils;
import edu.berkeley.nlp.util.Factory;
import edu.berkeley.nlp.util.Filter;
import edu.berkeley.nlp.util.Filters;
import edu.berkeley.nlp.util.Pair;

/**
 * TODO Add tag reading
 * 
 * @author John DeNero & Percy Liang
 */
public class SentencePairReader
{

	/**
	 * A PairDepot iterates through sentence pairs that have already been loaded
	 * into memory if they are available, or from disk if they are not.
	 */
	public class PairDepot implements Iterable<SentencePair>
	{

		private List<String> sources;

		private int offset;

		private int maxPairs;

		private Filter<SentencePair> filter;

		private List<SentencePair> pairs;

		private int size = -1;

		private boolean ignoreAnnotations;

		public PairDepot(List<String> sources, int offset, int maxPairs, Filter<SentencePair> filter, boolean batch) {
			this(sources, offset, maxPairs, filter, batch, false);
		}

		public SentencePairReader getSentencePairReader() {
			return SentencePairReader.this;
		}

		public PairDepot(List<String> sources, int offset, int maxPairs, Filter<SentencePair> filter, boolean batch, boolean ignoreAnnotations) {
			this.sources = sources;
			this.offset = offset;
			this.maxPairs = maxPairs;
			this.filter = filter;
			this.ignoreAnnotations = ignoreAnnotations;
			if (!batch) loadSentenceCache();
		}

		public int size() {
			if (size >= 0) return size;
			if (pairs != null) {
				size = pairs.size();
			} else {
				int count = 0;
				Iterator<SentencePair> it = this.iterator();
				while (it.hasNext()) {
					it.next();
					count++;
				}
				size = count;
			}
			return size;
		}

		public Iterator<SentencePair> iterator() {
			// If pairs are loaded already, return them
			if (pairs != null) return pairs.iterator();

			// Factory to generate iterators from source strings
			Factory<String, Iterator<SentencePair>> factory = new Factory<String, Iterator<SentencePair>>()
			{
				public Iterator<SentencePair> newInstance(String src) {
					return getSentencePairIteratorFromSource(src, filter, ignoreAnnotations);
				}
			};

			// Create the iterator (which iterates over sources internally)
			Iterator<SentencePair> iter = new CollectionUtils.IteratorIterator<String, SentencePair>(sources.iterator(), factory);

			// Burn off the offset
			for (int i = 0; i < offset; i++) {
				if (iter.hasNext()) {
					iter.next();
				} else {
					Logger.err("Pairs available (%d) less than offset (%d)", i, offset);
					break;
				}
			}

			return CollectionUtils.maxLengthIterator(iter, maxPairs);
		}

		/**
		 * @return
		 */
		public List<SentencePair> asList() {
			if (pairs != null) return pairs;
			ArrayList<SentencePair> allPairs = new ArrayList<SentencePair>(Math.max(0, size));
			for (SentencePair pair : this) {
				allPairs.add(pair);
			}
			return allPairs;
		}

		/**
         *
         */
		public void flushCache() {
			pairs = null;
		}

		/**
         *
         */
		public List<SentencePair> loadSentenceCache() {
			pairs = asList();
			return pairs;
		}

		public List<String> getSources() {
			return sources;
		}

		public List<Pair<String, Iterator<SentencePair>>> getSourceIterators() {
			List<Pair<String, Iterator<SentencePair>>> iters;
			iters = new ArrayList<Pair<String, Iterator<SentencePair>>>();
			for (String source : sources) {
				List<String> files = getBaseFileNamesFromSource(source);
				for (String f : files) {
					Iterator<SentencePair> it = getSentencePairsIteratorFromFile(f, filter);
					iters.add(Pair.makePair(f, it));
				}
			}
			return iters;
		}
	}

	public class PairIterator implements Iterator<SentencePair>
	{
		private String baseFileName;

		private Filter<SentencePair> filter;

		// Input files
		private String englishFN;

		private String foreignFN;

		// Input files
		private BufferedReader englishIn;

		private BufferedReader frenchIn;

		private SentencePair next;

		private boolean isEmpty;

		private void setInputFileNames(String baseFileName) {
			englishFN = baseFileName + "." + englishExtension;
			foreignFN = baseFileName + "." + foreignExtension;
		}

		private SentencePair readNextSentencePair(String englishLine, String frenchLine, String baseFileName) {
			Pair<Integer, List<String>> englishIDAndSentence = readSentence(englishLine);
			Pair<Integer, List<String>> frenchIDAndSentence = readSentence(frenchLine);

			int enID = englishIDAndSentence.getFirst();
			int frID = frenchIDAndSentence.getFirst();
			if (enID != frID)
				throw new RuntimeException("Sentence ID confusion in file " + baseFileName + ", lines were:\n\t" + englishLine + "\n\t" + frenchLine);
			if (enID == -1) enID = frID = currSentenceID;
			SentencePair sp = new SentencePair(enID, baseFileName, englishIDAndSentence.getSecond(), frenchIDAndSentence.getSecond());
			return sp;
		}

		private Pair<Integer, List<String>> readSentence(String line) {
			int id = -1;
			List<String> words = new ArrayList<String>();
			String[] tokens = line.trim().split("\\s+");
			for (int i = 0; i < tokens.length; i++) {
				String token = tokens[i];
				if (token.equals("<s")) continue;
				if (token.equals("</s>")) continue;
				if (token.startsWith("snum=")) {
					String idString = token.substring(5, token.length() - 1);
					id = Integer.parseInt(idString);
					continue;
				}
				words.add(token.intern());
			}

			return new Pair<Integer, List<String>>(id, words);
		}

		public PairIterator(String baseFileName, Filter<SentencePair> filter, boolean ignoreAnnotations) {
			this.baseFileName = new File(baseFileName).getPath();
			this.filter = filter;
			setInputFileNames(baseFileName);

			// Open all relevant files, setting a dead state if they don't exist
			englishIn = IOUtils.openInHard(englishFN);
			frenchIn = IOUtils.openInHard(foreignFN);

			loadNext();

		}

		public PairIterator(String enFile, String foreignFile, String alignFile, String englishTreesFile) {
			this.baseFileName = "";
			this.filter = Filters.acceptFilter();
			englishFN = enFile;
			foreignFN = foreignFile;
			englishIn = IOUtils.openInHard(enFile);
			frenchIn = IOUtils.openInHard(foreignFN);

			loadNext();

		}

		public boolean hasNext() {
			if (isEmpty) return false;
			return next != null;
		}

		public SentencePair next() {
			SentencePair output = next;
			loadNext();
			return output;
		}

		private void loadNext() {
			try {
				next = null;
				if (englishIn.ready() != frenchIn.ready()) {
					Logger.warn("%s and %s files are different lengths (%s)", englishExtension, foreignExtension, baseFileName);
				}
				while (next == null && englishIn.ready() && frenchIn.ready()) {
					// Check bounds on desired sentences
					String englishLine = englishIn.readLine();
					String frenchLine = frenchIn.readLine();
					currSentenceID--;

					// Construct sentence pair
					SentencePair pair = readNextSentencePair(englishLine, frenchLine, baseFileName);

					// TODO Support tag input

					if (filter.accept(pair)) {
						next = pair;
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

		public void skipNext() {
			try {
				englishIn.readLine();
				frenchIn.readLine();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			currSentenceID--;
		}

		public void finish() {
		}

	}

	private String englishExtension = "en";

	public String getEnglishExtension() {
		return englishExtension;
	}

	public String getForeignExtension() {
		return foreignExtension;
	}

	private String foreignExtension = "fr";

	private static int currSentenceID = 0;

	/**
	 * Creates a sentence pair reader that does not save inputs.
	 * 
	 * @param lowercase
	 */
	public SentencePairReader() {
	}

	public static void assertSentenceIDsAreUnique(List<SentencePair> sentencePairs) {
		Map<Integer, SentencePair> map = new HashMap<Integer, SentencePair>();
		for (SentencePair sp : sentencePairs) {
			int sid = sp.getSentenceID();
			if (map.containsKey(sid)) { throw new RuntimeException("Two sentences have same sentence ID: " + sid); }
			map.put(sid, sp);
		}
	}

	//	/**
	//	 * Reads translation data (sentence pairs) from a set of parallel files. The
	//	 * files can include several sources of information, including tags, trees
	//	 * and word alignments.
	//	 * 
	//	 * @param path
	//	 *            Input directory path
	//	 * @param maxSentencePairs
	//	 *            Number of sentences to return
	//	 * @param sentencePairs
	//	 *            Database of sentence pairs
	//	 */
	//	@SuppressWarnings("unchecked")
	//	public void readSentencePairsFromSource(String path, int maxSentencePairs, List<SentencePair> sentencePairs) {
	//		readSentencePairsFromSource(path, 0, maxSentencePairs, sentencePairs, Filters.<SentencePair> acceptFilter());
	//	}

	public Iterator<SentencePair> getSentencePairIteratorFromSource(String path, final Filter<SentencePair> f, final boolean ignoreAnnotations) {
		// Load file names
		final List<String> filenames = getBaseFileNamesFromSource(path);
		if (filenames.size() == 0) Logger.err("No files found at source " + path);
		Collections.sort(filenames);
		// Create iterator factory from file names
		Factory<String, Iterator<SentencePair>> factory = new Factory<String, Iterator<SentencePair>>()
		{
			public Iterator<SentencePair> newInstance(String filename) {
				return new PairIterator(filename, f, ignoreAnnotations);
			}
		};

		return new CollectionUtils.IteratorIterator<String, SentencePair>(filenames.iterator(), factory);
	}

	public PairDepot pairDepotFromSources(List<String> sources, int offset, int numSentences, Filter<SentencePair> filter, boolean batch) {
		return new PairDepot(sources, offset, numSentences, filter, batch);
	}

	public PairDepot pairDepotFromSources(List<String> sources, int numSentences) {
		return pairDepotFromSources(sources, 0, numSentences, Filters.<SentencePair> acceptFilter(), false);
	}

	public PairDepot pairDepotFromSources(List<String> sources, int offset, int numSentences, Filter<SentencePair> filter, boolean batch,
		boolean ignoreAnnotations) {
		return new PairDepot(sources, offset, numSentences, filter, batch, ignoreAnnotations);
	}

	public PairDepot pairDepotFromSource(String source, int offset, int numSentences, Filter<SentencePair> filter, boolean batch) {
		ArrayList<String> sources = new ArrayList<String>(1);
		sources.add(source);
		return new PairDepot(sources, offset, numSentences, filter, batch);
	}

	public PairDepot pairDepotFromSource(String source, int numSentences) {
		return pairDepotFromSource(source, 0, numSentences, Filters.<SentencePair> acceptFilter(), false);
	}

	//	/**
	//	 * Reads translation data (sentence pairs) from a set of parallel files. The
	//	 * files can include several sources of information, including tags, trees
	//	 * and word alignments.
	//	 * 
	//	 * @param path
	//	 *            Input directory path
	//	 * @param offset
	//	 *            Number of sentences to skip at outset
	//	 * @param maxSentencePairs
	//	 *            Number of sentences to return
	//	 * @param sentencePairs
	//	 *            Database of sentence pairs
	//	 * @param filter
	//	 *            Filter for sentence pairs
	//	 */
	//	public void readSentencePairsFromSource(String path, int offset, int maxSentencePairs, List<SentencePair> sentencePairs, Filter<SentencePair> filter) {
	//		Logger.startTrack("readSentencePairs(" + path + ")");
	//		int startCount = sentencePairs.size();
	//		List<String> filenames = getBaseFileNamesFromSource(path);
	//		Collections.sort(filenames);
	//		readSentencePairsUsingList(filenames, offset, maxSentencePairs, sentencePairs, filter);
	//		Logger.logss("Finished reading %d sentences", sentencePairs.size() - startCount);
	//		Logger.endTrack();
	//	}

	// If path is a directory, return the list of files in the directory
	// If path is a file, return the files whose names are in the path file
	private List<String> getBaseFileNamesFromSource(String path) {
		if (new File(path).isDirectory()) {
			return getBaseFileNamesFromDir(path);

		} else {
			throw new RuntimeException(path + " could not be found or is not a directory.");
		}
	}

	private List<String> getBaseFileNamesFromDir(String path) {
		SuffixFilter filter = new SuffixFilter(englishExtension);
		List<File> englishFiles = IOUtils.getFilesUnder(path, filter);
		List<String> baseFileNames = new ArrayList<String>();
		for (File englishFile : englishFiles) {
			String baseFileName = chop(englishFile.getAbsolutePath(), "." + englishExtension);
			baseFileNames.add(baseFileName);
		}
		return baseFileNames;
	}

	//	private void readSentencePairsUsingList(List<String> baseFileNames, int offset, int maxSentencePairs, List<SentencePair> sentencePairs,
	//		Filter<SentencePair> filter) {
	//		initSaveDirectories();
	//
	//		int numFiles = 0;
	//		for (String baseFileName : baseFileNames) {
	//			if (sentencePairs.size() >= maxSentencePairs) return;
	//			numFiles++;
	//			Logger.logs("Reading " + numFiles + "/" + baseFileNames.size() + ": " + baseFileName);
	//			List<SentencePair> subSentencePairs = readSentencePairsFromFile(baseFileName, maxSentencePairs - sentencePairs.size(), filter);
	//			int lowerBound = NumUtils.bound(offset, 0, subSentencePairs.size());
	//			sentencePairs.addAll(subSentencePairs.subList(lowerBound, subSentencePairs.size()));
	//			offset -= Math.max(0, subSentencePairs.size());
	//		}
	//	}

	//	private void initSaveDirectories() {
	//		// Choose whether to save inputs
	//		if (saveInputDir != null) {
	//			File inputDir = new File(saveInputDir);
	//			if (!inputDir.exists()) {
	//				inputDir.mkdir();
	//			}
	//			if (inputDir.exists() && inputDir.isDirectory()) saveInput = true;
	//		}
	//
	//		// Choose whether to save rejects
	//		if (saveRejectsDir != null) {
	//			File rejectsDir = new File(saveRejectsDir);
	//			if (!rejectsDir.exists()) {
	//				rejectsDir.mkdir();
	//			}
	//			if (rejectsDir.exists() && rejectsDir.isDirectory()) saveRejects = true;
	//		}
	//	}

	// For sentence pairs before offset, just stick null instead of the actual sentence
	public List<SentencePair> readSentencePairsFromFile(String baseFileName, int maxSentencePairs, Filter<SentencePair> filter) {
		List<SentencePair> sentencePairs = new ArrayList<SentencePair>();
		PairIterator pairIterator = getSentencePairsIteratorFromFile(baseFileName, filter);

		// Process offset
		//		for (int i = 0; i < offset; i++) {
		//			sentencePairs.add(null);
		//			if (pairIterator.hasNext())
		//				pairIterator.skipNext();
		//			else
		//				return sentencePairs;
		//		}

		int numPairs = 0;
		while (pairIterator.hasNext() && numPairs < maxSentencePairs) {
			sentencePairs.add(pairIterator.next());
			numPairs++;
		}
		pairIterator.finish();
		return sentencePairs;
	}

	public PairIterator getSentencePairsIteratorFromFile(String baseFileName, Filter<SentencePair> filter) {
		return new PairIterator(baseFileName, filter, false);
	}

	private static String chop(String name, String extension) {
		if (!name.endsWith(extension)) return name;
		return name.substring(0, name.length() - extension.length());
	}

	public void setEnglishExtension(String ext) {
		this.englishExtension = ext;
	}

	public void setForeignExtension(String ext) {
		this.foreignExtension = ext;
	}

	public Iterable<SentencePair> newPairReader(final String englishFile, final String foreignFile, final String alignFile, final String enTreeFile) {
		return new Iterable<SentencePair>()
		{

			public Iterator<SentencePair> iterator() {
				return new PairIterator(englishFile, foreignFile, alignFile, enTreeFile);
			}

		};
	}

}

