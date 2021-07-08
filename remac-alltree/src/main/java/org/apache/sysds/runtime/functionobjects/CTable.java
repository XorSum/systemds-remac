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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.matrix.data.CTableMap;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.Pair;
import org.apache.sysds.runtime.util.UtilFunctions;

public class CTable extends ValueFunction 
{
	private static final long serialVersionUID = -5374880447194177236L;

	private static CTable singleObj = null;
	
	private CTable() {
		// nothing to do here
	}
	
	public static CTable getCTableFnObject() {
		if ( singleObj == null )
			singleObj = new CTable();
		return singleObj;
	}

	public void execute(double v1, double v2, double w, boolean ignoreZeros, CTableMap resultMap, MatrixBlock resultBlock) {
		if( resultBlock != null )
			execute(v1, v2, w, ignoreZeros, resultBlock);
		else
			execute(v1, v2, w, ignoreZeros, resultMap);
	}
	
	public void execute(double v1, double v2, double w, boolean ignoreZeros, CTableMap resultMap) {
		// If any of the values are NaN (i.e., missing) then 
		// we skip this tuple, proceed to the next tuple
		if ( Double.isNaN(v1) || Double.isNaN(v2) || Double.isNaN(w) ) {
			return;
		}
		
		// safe casts to long for consistent behavior with indexing
		long row = UtilFunctions.toLong( v1 );
		long col = UtilFunctions.toLong( v2 );
		
		// skip this entry as it does not fall within specified output dimensions
		if( ignoreZeros && row == 0 && col == 0 ) {
			return;
		}
		
		//check for incorrect ctable inputs
		if( row <= 0 || col <= 0 ) {
			throw new DMLRuntimeException("Erroneous input while computing the contingency table (one of the value <= zero): "+v1+" "+v2);
		} 
	
		//hash group-by for core ctable computation
		resultMap.aggregate(row, col, w);	
	}	

	public void execute(double v1, double v2, double w, boolean ignoreZeros, MatrixBlock ctableResult) 
	{
		// If any of the values are NaN (i.e., missing) then 
		// we skip this tuple, proceed to the next tuple
		if ( Double.isNaN(v1) || Double.isNaN(v2) || Double.isNaN(w) ) {
			return;
		}
		
		// safe casts to long for consistent behavior with indexing
		long row = UtilFunctions.toLong( v1 );
		long col = UtilFunctions.toLong( v2 );
		
		// skip this entry as it does not fall within specified output dimensions
		if( ignoreZeros && row == 0 && col == 0 ) {
			return;
		}
		
		//check for incorrect ctable inputs
		if( row <= 0 || col <= 0 ) {
			throw new DMLRuntimeException("Erroneous input while computing the contingency table (one of the value <= zero): "+v1+" "+v2);
		}
		
		// skip this entry as it does not fall within specified output dimensions
		if( row > ctableResult.getNumRows() || col > ctableResult.getNumColumns() ) {
			return;
		}
		
		//add value
		ctableResult.quickSetValue((int)row-1, (int)col-1,
				ctableResult.quickGetValue((int)row-1, (int)col-1) + w);
	}

	public int execute(int row, double v2, double w, int maxCol, MatrixBlock ctableResult) 
	{
		// If any of the values are NaN (i.e., missing) then 
		// we skip this tuple, proceed to the next tuple
		if ( Double.isNaN(v2) || Double.isNaN(w) ) {
			return maxCol;
		}
		
		// safe casts to long for consistent behavior with indexing
		int col = UtilFunctions.toInt( v2 );
		if( col <= 0 ) {
			throw new DMLRuntimeException("Erroneous input while computing the contingency table (value <= zero): "+v2);
		} 
		
		//set weight as value (expand is guaranteed to address different cells)
		ctableResult.quickSetValue(row-1, col-1, w);
		
		//maintain max seen col 
		return Math.max(maxCol, col);
	}

	public Pair<MatrixIndexes,Double> execute( long row, double v2, double w ) 
	{
		// If any of the values are NaN (i.e., missing) then 
		// we skip this tuple, proceed to the next tuple
		if ( Double.isNaN(v2) || Double.isNaN(w) )
			return new Pair<>(new MatrixIndexes(-1,-1), w);
		// safe casts to long for consistent behavior with indexing
		long col = UtilFunctions.toLong( v2 );
		if( col <= 0 )
			throw new DMLRuntimeException("Erroneous input while computing the contingency table (value <= zero): "+v2);
		return new Pair<>(new MatrixIndexes(row, col), w);
	}
}
