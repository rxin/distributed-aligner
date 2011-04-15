package edu.berkeley.nlp.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.WeakHashMap;

/**
 * The MapFactory is a mechanism for specifying what kind of map is to be used
 * by some object. For example, if you want a Counter which is backed by an
 * IdentityHashMap instead of the defaul HashMap, you can pass in an
 * IdentityHashMapFactory.
 * 
 * @author Dan Klein
 */

public abstract class MapFactory<K, V> implements Serializable
{
	private static final long serialVersionUID = 5724671156522771657L;

	public static class HashMapFactory<K, V> extends MapFactory<K, V>
	{
		@Override
		public Map<K, V> buildMap() {
			return new HashMap<K, V>();
		}
	}

	public static class IdentityHashMapFactory<K, V> extends MapFactory<K, V>
	{
		private int expSize;

		public IdentityHashMapFactory(int expSize) {
			this.expSize = expSize;
		}

		@Override
		public Map<K, V> buildMap() {
			return new IdentityHashMap<K, V>(expSize);
		}
	}

	public static class TreeMapFactory<K, V> extends MapFactory<K, V>
	{
		@Override
		public Map<K, V> buildMap() {
			return new TreeMap<K, V>();
		}
	}

	public static class WeakHashMapFactory<K, V> extends MapFactory<K, V>
	{
		@Override
		public Map<K, V> buildMap() {
			return new WeakHashMap<K, V>();
		}
	}

	public abstract Map<K, V> buildMap();
}

