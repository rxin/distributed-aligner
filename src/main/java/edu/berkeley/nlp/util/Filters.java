package edu.berkeley.nlp.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Filters contains some simple implementations of the Filter interface.
 * 
 * @author Christopher Manning
 * @version 1.0
 */
public class Filters
{

	/**
	 * Nothing to instantiate
	 */
	private Filters() {
	}

	/**
	 * The acceptFilter accepts everything.
	 */
	public static <T> Filter<T> acceptFilter() {
		return new CategoricalFilter<T>(true);
	}

	private static final class CategoricalFilter<T> implements Filter<T>
	{

		private final boolean judgment;

		private CategoricalFilter(boolean judgment) {
			this.judgment = judgment;
		}

		/**
		 * Checks if the given object passes the filter.
		 * 
		 * @param obj
		 *            an object to test
		 */
		public boolean accept(T obj) {
			return judgment;
		}
	}

}

