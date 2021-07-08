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

package org.apache.sysds.runtime.functionobjects;

import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.runtime.meta.DataCharacteristics;


public class ReduceRow extends IndexFunction
{

	private static final long serialVersionUID = 3611700251686303484L;

	private static ReduceRow singleObj = null;
	
	private ReduceRow() {
		// nothing to do here
	}
	
	public static ReduceRow getReduceRowFnObject() {
		if ( singleObj == null )
			singleObj = new ReduceRow();
		return singleObj;
	}
	
	/*
	 * NOTE: index starts from 1 for cells in a matrix, but index starts from 0 for cells inside a block
	 */
	
	@Override
	public void execute(MatrixIndexes in, MatrixIndexes out) {
		out.setIndexes(1, in.getColumnIndex());
	}

	@Override
	public void execute(CellIndex in, CellIndex out) {
		out.row=0;
		out.column=in.column;
	}

	@Override
	public boolean computeDimension(int row, int col, CellIndex retDim) {
		retDim.set(1, col);
		return true;
	}
	
	@Override
	public boolean computeDimension(DataCharacteristics in, DataCharacteristics out) {
		out.set(1, in.getCols(), in.getBlocksize());
		return true;
	}

}
