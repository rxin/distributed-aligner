package edu.berkeley.nlp.util;

public interface Factory<E, T>
{
	T newInstance(E arg);

}

