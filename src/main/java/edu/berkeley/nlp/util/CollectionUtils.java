package edu.berkeley.nlp.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Dan Klein
 */
public class CollectionUtils
{
	public static <E extends Comparable<E>> List<E> sort(Collection<E> c) {
		List<E> list = new ArrayList<E>(c);
		Collections.sort(list);
		return list;
	}

	public static <E> List<E> sort(Collection<E> c, Comparator<E> r) {
		List<E> list = new ArrayList<E>(c);
		Collections.sort(list, r);
		return list;
	}

	public static <K, V> void addToValueList(Map<K, List<V>> map, K key, V value) {
		List<V> valueList = map.get(key);
		if (valueList == null) {
			valueList = new ArrayList<V>();
			map.put(key, valueList);
		}
		valueList.add(value);
	}

	public static <K, V> List<V> getValueList(Map<K, List<V>> map, K key) {
		List<V> valueList = map.get(key);
		if (valueList == null) return Collections.emptyList();
		return valueList;
	}

	public static <E> List<E> iteratorToList(Iterator<E> iterator) {
		List<E> list = new ArrayList<E>();
		while (iterator.hasNext()) {
			list.add(iterator.next());
		}
		return list;
	}

	public static <E> Set<E> union(Set<? extends E> x, Set<? extends E> y) {
		Set<E> union = new HashSet<E>();
		union.addAll(x);
		union.addAll(y);
		return union;
	}

	/**
	 * Convenience method for constructing lists on one line. Does type
	 * inference:
	 * <code>List<String> args = makeList("-length", "20","-parser","cky");</code>
	 * 
	 * @param <T>
	 * @param elems
	 * @return
	 */
	public static <T> List<T> makeList(T... elems) {
		List<T> list = new ArrayList<T>();
		for (T elem : elems) {
			list.add(elem);
		}
		return list;
	}

	public static long sum(long[] a) {
		if (a == null) { return 0; }
		long result = 0;
		for (int i = 0; i < a.length; i++) {
			result += a[i];
		}
		return result;
	}

	/**
	 * Wraps an iterator as an iterable
	 * 
	 * @param <T>
	 * @param it
	 * @return
	 */
	public static <T> Iterable<T> iterable(final Iterator<T> it) {
		return new Iterable<T>()
		{
			boolean used = false;

			public Iterator<T> iterator() {
				if (used) throw new RuntimeException("One use iterable");
				used = true;
				return it;
			}
		};
	}

	public static long[] copyOf(long[] a, int length) {
		long[] ret = new long[length];
		System.arraycopy(a, 0, ret, 0, Math.min(ret.length, a.length));
		return ret;
	}

	public static int[] copyOf(int[] a, int length) {
		int[] ret = new int[length];
		System.arraycopy(a, 0, ret, 0, Math.min(ret.length, a.length));
		return ret;
	}

	public static double[] copyOf(double[] a, int length) {
		double[] ret = new double[length];
		System.arraycopy(a, 0, ret, 0, Math.min(ret.length, a.length));
		return ret;
	}

	public static int[] copyOfRange(int[] a, int from, int to) {
		int[] ret = new int[to - from];
		System.arraycopy(a, from, ret, 0, ret.length);
		return ret;
	}

	public static void fill(boolean[][] a, boolean b) {
		for (boolean[] c : a) {
			if (c != null) Arrays.fill(c, b);
		}
	}

	public static void fill(int[][] a, int i) {
		for (int[] c : a) {
			if (c != null) Arrays.fill(c, i);
		}
	}

	public static <S, T> Iterable<Pair<S, T>> zip(Pair<? extends Iterable<S>, ? extends Iterable<T>> input) {
		final Iterator<S> firstIterator = input.getFirst().iterator();
		final Iterator<T> secondIterator = input.getSecond().iterator();
		return CollectionUtils.iterable(new Iterator<Pair<S, T>>()
		{

			public boolean hasNext() {
				return firstIterator.hasNext() && secondIterator.hasNext();
			}

			public Pair<S, T> next() {
				return Pair.makePair(firstIterator.next(), secondIterator.next());
			}

			public void remove() {

				throw new UnsupportedOperationException("Remove not supported");
			}
		});
	}

	/**
	 * Wraps a two-level iteration scenario in an iterator. Each key of the keys
	 * iterator returns an iterator (via the factory) over T's.
	 * 
	 * The IteratorIterator loops through the iterator associated with each key
	 * until all the keys are used up.
	 */
	public static class IteratorIterator<E, T> implements Iterator<T>
	{
		Iterator<T> current = null;

		Iterator<E> keys;

		Factory<E, Iterator<T>> iterFactory;

		public IteratorIterator(Iterator<E> keys, Factory<E, Iterator<T>> iterFactory) {
			this.keys = keys;
			this.iterFactory = iterFactory;
			current = getNextIterator();
		}

		private Iterator<T> getNextIterator() {
			Iterator<T> next = null;
			while (next == null) {
				if (!keys.hasNext()) break;
				next = iterFactory.newInstance(keys.next());
				if (!next.hasNext()) next = null;
			}
			return next;
		}

		public boolean hasNext() {
			return current != null;
		}

		public T next() {
			T next = current.next();
			if (!current.hasNext()) current = getNextIterator();
			return next;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Provides a max number of elements for an underlying base iterator.
	 */
	public static <T> Iterator<T> maxLengthIterator(final Iterator<T> base, final int max) {
		return new Iterator<T>()
		{
			int count = 0;

			public boolean hasNext() {
				return base.hasNext() && count < max;
			}

			public T next() {
				if (!hasNext()) throw new NoSuchElementException("No more elements");
				count++;
				return base.next();
			}

			public void remove() {
				throw new UnsupportedOperationException();
				// TODO Maybe this should behave in a more friendly manner
			}

		};
	}

	public static <T> Iterable<T> concat(final Iterable<T> a, final Iterable<T> b) {
		return new Iterable<T>() { public Iterator<T> iterator() { return concat(a.iterator(), b.iterator());}};
	}

	public static <T> Iterator<T> concat(Iterable<Iterator<T>> args) {
		Factory<Iterator<T>, Iterator<T>> factory = new Factory<Iterator<T>, Iterator<T>>()
		{

			public Iterator<T> newInstance(Iterator<T> args) {
				return (Iterator<T>) args;
			}

		};
		return new IteratorIterator<Iterator<T>, T>(args.iterator(), factory);
	}

	public static <T> Iterator<T> concat(Iterator<T>... args) {
		Factory<Iterator<T>, Iterator<T>> factory = new Factory<Iterator<T>, Iterator<T>>()
		{

			public Iterator<T> newInstance(Iterator<T> arg) {
				return (Iterator<T>) arg;
			}

		};
		return new IteratorIterator<Iterator<T>, T>(Arrays.asList(args).iterator(), factory);
	}

	/**
	 * Wraps a base iterator with a transformation function.
	 */
	public static abstract class Transform<S, T> implements Iterator<T>
	{

		private Iterator<S> base;

		public Transform(Iterator<S> base) {
			this.base = base;
		}

		public boolean hasNext() {
			return base.hasNext();
		}

		public T next() {
			return transform(base.next());
		}

		protected abstract T transform(S next);

		public void remove() {
			base.remove();
		}

	}

}

