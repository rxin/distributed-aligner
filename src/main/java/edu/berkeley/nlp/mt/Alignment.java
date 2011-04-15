package edu.berkeley.nlp.mt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.berkeley.nlp.mt.Alignment.Link;
import edu.berkeley.nlp.mt.SentencePair;
import edu.berkeley.nlp.util.Pair;

/**
 * Alignments serve two purposes, both to indicate your system's guessed
 * alignment, and to hold the gold standard alignments. Alignments map index
 * pairs to one of three values, unaligned, possibly aligned, and surely
 * aligned. Your alignment guesses should only contain sure and unaligned pairs,
 * but the gold alignments contain possible pairs as well.
 * 
 * To build an alignemnt, start with an empty one and use
 * addAlignment(i,j,true). To display one, use the render method.
 */
public class Alignment
{
	Set<Pair<Integer, Integer>> sureAlignments;

	Set<Pair<Integer, Integer>> possibleAlignments;

	public boolean containsSureAlignment(int englishPosition, int frenchPosition) {
		return sureAlignments.contains(new Pair<Integer, Integer>(englishPosition, frenchPosition));
	}

	public boolean containsPossibleAlignment(int englishPosition, int frenchPosition) {
		return possibleAlignments.contains(new Pair<Integer, Integer>(englishPosition, frenchPosition));
	}

	public void addAlignment(int englishPosition, int frenchPosition, boolean sure) {
		if (englishPosition < 0 || frenchPosition < 0) return;
		Pair<Integer, Integer> alignment = new Pair<Integer, Integer>(englishPosition, frenchPosition);
		if (sure) sureAlignments.add(alignment);
		possibleAlignments.add(alignment);
	}

	public Alignment() {
		sureAlignments = new HashSet<Pair<Integer, Integer>>();
		possibleAlignments = new HashSet<Pair<Integer, Integer>>();
	}

	public static String render(Alignment alignment, SentencePair sentencePair) {
		return render(alignment, alignment, sentencePair);
	}

	public static String render(Alignment reference, Alignment proposed, SentencePair sentencePair) {
		StringBuilder sb = new StringBuilder();
		for (int frenchPosition = 0; frenchPosition < sentencePair.getFrenchWords().size(); frenchPosition++) {
			for (int englishPosition = 0; englishPosition < sentencePair.getEnglishWords().size(); englishPosition++) {
				boolean sure = reference.containsSureAlignment(englishPosition, frenchPosition);
				boolean possible = reference.containsPossibleAlignment(englishPosition, frenchPosition);
				char proposedChar = ' ';
				if (proposed.containsSureAlignment(englishPosition, frenchPosition)) proposedChar = '#';
				if (sure) {
					sb.append('[');
					sb.append(proposedChar);
					sb.append(']');
				} else {
					if (possible) {
						sb.append('(');
						sb.append(proposedChar);
						sb.append(')');
					} else {
						sb.append(' ');
						sb.append(proposedChar);
						sb.append(' ');
					}
				}
			}
			sb.append("| ");
			sb.append(sentencePair.getFrenchWords().get(frenchPosition));
			sb.append('\n');
		}
		for (int englishPosition = 0; englishPosition < sentencePair.getEnglishWords().size(); englishPosition++) {
			sb.append("---");
		}
		sb.append("'\n");
		boolean printed = true;
		int index = 0;
		while (printed) {
			printed = false;
			StringBuilder lineSB = new StringBuilder();
			for (int englishPosition = 0; englishPosition < sentencePair.getEnglishWords().size(); englishPosition++) {
				String englishWord = sentencePair.getEnglishWords().get(englishPosition);
				if (englishWord.length() > index) {
					printed = true;
					lineSB.append(' ');
					lineSB.append(englishWord.charAt(index));
					lineSB.append(' ');
				} else {
					lineSB.append("   ");
				}
			}
			index += 1;
			if (printed) {
				sb.append(lineSB);
				sb.append('\n');
			}
		}
		return sb.toString();
	}

	public static Map<Integer, Alignment> readAlignments(String fileName) {
		Map<Integer, Alignment> alignments = new HashMap<Integer, Alignment>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			while (in.ready()) {
				String line = in.readLine();
				String[] words = line.split("\\s+");
				if (words.length != 4) throw new RuntimeException("Bad alignment file " + fileName + ", bad line was " + line);
				Integer sentenceID = Integer.parseInt(words[0]);
				Integer englishPosition = Integer.parseInt(words[1]) - 1;
				Integer frenchPosition = Integer.parseInt(words[2]) - 1;
				String type = words[3];
				Alignment alignment = alignments.get(sentenceID);
				if (alignment == null) {
					alignment = new Alignment();
					alignments.put(sentenceID, alignment);
				}
				alignment.addAlignment(englishPosition, frenchPosition, type.equals("S"));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return alignments;
	}

	public enum LinkState
	{
		SURE, POSS, OFF;
	}

	public static final class Link
	{
		public final int en;

		public final int fr;

		public final LinkState state;

		public final double strength;

		public final boolean hasStrength;

		public Link(int en, int fr, double strength) {
			this.en = en;
			this.fr = fr;
			this.state = LinkState.OFF;
			this.strength = strength;
			this.hasStrength = true;
		}

		public Link(int en, int fr, boolean sure) {
			super();
			this.en = en;
			this.fr = fr;
			this.state = sure ? LinkState.SURE : LinkState.POSS;
			this.strength = 0;
			this.hasStrength = false;
		}

		public Link(int en, int fr, LinkState state, double strength) {
			this.en = en;
			this.fr = fr;
			this.state = state;
			this.strength = strength;
			this.hasStrength = true;
		}

		/**
		 * For hash lookups
		 */
		public Link(int en, int fr) {
			this.en = en;
			this.fr = fr;
			this.state = LinkState.OFF;
			this.strength = 0;
			this.hasStrength = false;
		}

		public boolean isSure() {
			return state == LinkState.SURE;
		}

		public boolean isPoss() {
			return state == LinkState.POSS || state == LinkState.SURE;
		}

		@Override
		public String toString() {
			return fr + "-" + en + ((hasStrength) ? "-" + (strength) : (isSure() ? "" : "-P"));
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + en;
			result = prime * result + fr;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Link other = (Link) obj;
			if (en != other.en) return false;
			if (fr != other.fr) return false;
			return true;
		}
	}

	public List<Link> getAlignmentsToEnglish(int englishPos) {
		List<Link> englishAlignments = new ArrayList<Link>();
		for (Pair<Integer, Integer> al : sureAlignments) {
			if (al.getFirst() == englishPos) {
				englishAlignments.add(new Link(al.getFirst(), al.getSecond()));
			}
		}
		return englishAlignments;
	}

	public Set<Pair<Integer, Integer>> getSureAlignments() {
		return sureAlignments;
	}

	public Alignment getReverseCopy() {
		Alignment al = new Alignment();
		for (Pair<Integer, Integer> a : sureAlignments) {
			al.addAlignment(a.getSecond(), a.getFirst(), true);
		}
		for (Pair<Integer, Integer> a : possibleAlignments) {
			al.addAlignment(a.getSecond(), a.getFirst(), false);
		}
		return al;
	}

}

