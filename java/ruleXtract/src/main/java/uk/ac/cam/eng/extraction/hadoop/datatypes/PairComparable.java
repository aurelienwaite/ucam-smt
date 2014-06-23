/*******************************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use these files except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2014 - Juan Pino, Aurelien Waite, William Byrne
 *******************************************************************************/
package uk.ac.cam.eng.extraction.hadoop.datatypes;

import org.apache.hadoop.io.WritableComparable;

/**
 * A comparable pair than can be used for map reduce keys
 * 
 * @author Aurelien Waite
 * @date 28 May 2014
 * @param <F>
 * @param <S>
 */
@SuppressWarnings("rawtypes")
public class PairComparable<F extends WritableComparable, S extends WritableComparable>
		extends PairWritable<F, S> implements
		WritableComparable<PairComparable<F, S>> {

	public PairComparable(F first, S second) {
		super(first, second);
	}

	public PairComparable(Class<F> classF, Class<S> classS) {
		super(classF, classS);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(PairComparable<F, S> o) {
		int firstCompare = first.compareTo(o.first);
		if (firstCompare == 0) {
			return second.compareTo(o.second);
		} else {
			return firstCompare;
		}
	}

}
