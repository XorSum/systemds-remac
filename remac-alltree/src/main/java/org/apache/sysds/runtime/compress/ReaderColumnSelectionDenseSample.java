/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.compress;

import org.apache.sysds.runtime.compress.utils.DblArray;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

/** considers only a subset of row indexes */
public class ReaderColumnSelectionDenseSample extends ReaderColumnSelection {
	protected MatrixBlock _data;

	private int[] _sampleIndexes;
	private int lastIndex = -1;

	// reusable return
	private DblArray reusableReturn;
	private double[] reusableArr;

	public ReaderColumnSelectionDenseSample(MatrixBlock data, int[] colIndexes, int[] sampleIndexes,
		CompressionSettings compSettings) {
		super(colIndexes, -1, compSettings);
		_data = data;
		_sampleIndexes = sampleIndexes;
		reusableArr = new double[colIndexes.length];
		reusableReturn = new DblArray(reusableArr);
	}

	protected DblArray getNextRow() {
		if(lastIndex == _sampleIndexes.length - 1)
			return null;
		lastIndex++;
		for(int i = 0; i < _colIndexes.length; i++) {
			reusableArr[i] = _compSettings.transposeInput ? _data.quickGetValue(_colIndexes[i],
				_sampleIndexes[lastIndex]) : _data.quickGetValue(_sampleIndexes[lastIndex], _colIndexes[i]);
		}
		return reusableReturn;
	}

	@Override
	public int getCurrentRowIndex() {
		return _sampleIndexes[lastIndex];
	}
}
