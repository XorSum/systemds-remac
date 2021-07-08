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

package org.apache.sysds.parser;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.runtime.util.UtilFunctions;

public class IndexedIdentifier extends DataIdentifier 
{

	private static final Log LOG = LogFactory.getLog(DataExpression.class.getName());
	
	// stores the expressions containing the ranges for the 
	private Expression 	_rowLowerBound = null, _rowUpperBound = null, _colLowerBound = null, _colUpperBound = null;
	
	// stores whether row / col indices have same value (thus selecting either (1 X n) row-vector OR (n X 1) col-vector)
	private boolean _rowLowerEqualsUpper = false, _colLowerEqualsUpper = false;
	
	// 	for IndexedIdentifier, dim1 and dim2 will ultimately be the dimensions of the indexed region and NOT the dims of what is being indexed
	// 	E.g., for A[1:10,1:10], where A = Rand (rows = 20, cols = 20), dim1 = 10, dim2 = 10, origDim1 = 20, origDim2 = 20
	
	// stores the dimensions of Identifier prior to indexing 
	private long _origDim1, _origDim2;
	
	public boolean getRowLowerEqualsUpper(){
		return _rowLowerEqualsUpper;
	}
	
	public boolean getColLowerEqualsUpper() {
		return _colLowerEqualsUpper;
	}
	
	public void setRowLowerEqualsUpper(boolean passed){
		_rowLowerEqualsUpper  = passed;
	}
	
	public void setColLowerEqualsUpper(boolean passed) {
		_colLowerEqualsUpper = passed;
	}
	
	public IndexedIdentifier(String name, boolean passedRows, boolean passedCols){
		super(name);
		_rowLowerBound = null; 
   		_rowUpperBound = null; 
   		_colLowerBound = null; 
   		_colUpperBound = null;
   		
   		_rowLowerEqualsUpper = passedRows;
   		_colLowerEqualsUpper = passedCols;
   		
   		_origDim1 = -1L;
   		_origDim2 = -1L;
	}
	
	public IndexPair calculateIndexedDimensions(HashMap<String,DataIdentifier> ids, HashMap<String, ConstIdentifier> currConstVars, boolean conditional) 
	{
		// stores the updated row / col dimension info
		long updatedRowDim = -1, updatedColDim = -1;
		
		boolean isConst_rowLowerBound = false;
		boolean isConst_rowUpperBound = false;
		boolean isConst_colLowerBound = false;
		boolean isConst_colUpperBound = false;
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// STEP 1 : perform constant propagation for index boundaries
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// PROCESS ROW LOWER BOUND
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		// process row lower bound
		if (_rowLowerBound instanceof ConstIdentifier && ( _rowLowerBound instanceof IntIdentifier || _rowLowerBound instanceof DoubleIdentifier )){
			
			Long rowLB_1_1 = -1L;
			if (_rowLowerBound instanceof IntIdentifier) 
				rowLB_1_1 = ((IntIdentifier)_rowLowerBound).getValue();
			else 
				rowLB_1_1 = UtilFunctions.toLong(((DoubleIdentifier)_rowLowerBound).getValue());
				
			if (rowLB_1_1 < 1){
				raiseValidateError("lower-bound row index " + rowLB_1_1 + " initialized to out of bounds value. Value must be >= 1", conditional);
			}
			if ((this.getOrigDim1() >= 0)  && (rowLB_1_1 > this.getOrigDim1())) {
				raiseValidateError("lower-bound row index " + rowLB_1_1 + " initialized to out of bounds value.  Rows in " + this.getName() + ": " + this.getOrigDim1(), conditional);
			}
			// valid lower row bound value
			isConst_rowLowerBound = true;
		}
		else if (_rowLowerBound instanceof ConstIdentifier && !getDataType().isList() ) {
			raiseValidateError("assign lower-bound row index for Indexed Identifier " + this.toString() + " the non-numeric value " + _rowLowerBound.toString(), conditional);
		}
	
		// perform constant propogation for row lower bound
		else if (_rowLowerBound != null && _rowLowerBound instanceof DataIdentifier && !(_rowLowerBound instanceof IndexedIdentifier)) {
			String identifierName = ((DataIdentifier)_rowLowerBound).getName();
			
			// CASE: rowLowerBound is a constant DataIdentifier
			if (currConstVars.containsKey(identifierName) && !conditional){
				ConstIdentifier constValue = currConstVars.get(identifierName);
				
				if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
					LOG.info(this.printInfoLocation() + "attempted to assign lower row bound for " + this.toString() + "the non-numeric value " + constValue.getOutput().toString() + " assigned to " + identifierName + ". May cause runtime exception ");
	
				else {
					
					// test constant propogation			
					long tempRowLB = -1L;
					boolean validRowLB = true;
					if (constValue instanceof IntIdentifier) 
						tempRowLB = ((IntIdentifier)constValue).getValue();
					else
						tempRowLB = UtilFunctions.toLong(((DoubleIdentifier)constValue).getValue());
							
					if (tempRowLB < 1){
						LOG.info(this.printInfoLocation() + "lower-bound row index " + identifierName + " initialized to "  + tempRowLB + " May cause runtime exception (runtime value must be >= 1)");	
						validRowLB = false;
					}
					if (this.getOrigDim1() >= 0  && tempRowLB > this.getOrigDim1()){ 
						LOG.info(this.printInfoLocation() + "lower-bound row index " + identifierName 
								+ " initialized to " + tempRowLB + " May cause runtime exception (Rows in " 
								+ this.getName() + ": " + this.getOrigDim1() +")");
						validRowLB = false;
					}	
					
					if (validRowLB) {
						if (constValue instanceof IntIdentifier) {
							_rowLowerBound = new IntIdentifier((IntIdentifier) constValue, constValue);
						} else {
							_rowLowerBound = new DoubleIdentifier((DoubleIdentifier) constValue, constValue);
						}
						isConst_rowLowerBound = true;
					}
				} // end else -- if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))				
			} // end if (currConstVars.containsKey(identifierName))		
		} // end constant propogation for row LB
		
		// check 1 < indexed row lower-bound < rows in IndexedIdentifier 
		// (assuming row dims available for upper bound)
		Long rowLB_1 = -1L;
		if (isConst_rowLowerBound) {
				
			if (_rowLowerBound instanceof IntIdentifier) 
				rowLB_1 = ((IntIdentifier)_rowLowerBound).getValue();
			else
				rowLB_1 = UtilFunctions.toLong(((DoubleIdentifier)_rowLowerBound).getValue());
		}
			
		
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// PROCESS ROW UPPER BOUND
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (_rowUpperBound instanceof ConstIdentifier && ( _rowUpperBound instanceof IntIdentifier || _rowUpperBound instanceof DoubleIdentifier )){
			
			Long rowUB_1_1 = -1L;
			if (_rowUpperBound instanceof IntIdentifier) 
				rowUB_1_1 = ((IntIdentifier)_rowUpperBound).getValue();
			else 
				rowUB_1_1 = UtilFunctions.toLong(((DoubleIdentifier)_rowUpperBound).getValue());
				
			if (rowUB_1_1 < 1){
				raiseValidateError("upper-bound row index " + rowUB_1_1 + " out of bounds value. Value must be >= 1", conditional);
			}
			if ((this.getOrigDim1() >= 0)  && (rowUB_1_1 > this.getOrigDim1())) {
				raiseValidateError("upper-bound row index " + rowUB_1_1 + " out of bounds value.  Rows in " + this.getName() + ": " + this.getOrigDim1(), conditional);
			}
			if (isConst_rowLowerBound && rowUB_1_1 < rowLB_1){
				raiseValidateError("upper-bound row index " + rowUB_1_1 + " greater than lower-bound row index " + rowLB_1, conditional);
			}
			isConst_rowUpperBound = true;
		}	
		else if (_rowUpperBound instanceof ConstIdentifier && !getDataType().isList()){
			raiseValidateError("assign upper-bound row index for " + this.toString() + " the non-numeric value " + _rowUpperBound.toString(), conditional);
		}
		
		// perform constant propogation for upper row index
		else if (_rowUpperBound != null && _rowUpperBound instanceof DataIdentifier && !(_rowUpperBound instanceof IndexedIdentifier)) {
			String identifierName = ((DataIdentifier)_rowUpperBound).getName();
			
			if (currConstVars.containsKey(identifierName) && !conditional){
				ConstIdentifier constValue = currConstVars.get(identifierName);
				
				if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
					LOG.info(this.printInfoLocation() + "attempted to assign upper row bound for " + this.toString() + "the non-numeric value " + constValue.getOutput().toString() + " assigned to " + identifierName + ". May cause runtime exception ");
	
				else {						
					// test constant propogation			
					long tempRowUB = -1L;
					boolean validRowUB = true;
					if (constValue instanceof IntIdentifier) 
						tempRowUB = ((IntIdentifier)constValue).getValue();
					else
						tempRowUB = UtilFunctions.toLong(((DoubleIdentifier)constValue).getValue());
							
					if (tempRowUB < 1){
						LOG.info(this.printInfoLocation() + "upper-bound row index " + identifierName + " initialized to "  + tempRowUB + " May cause runtime exception (runtime value must be >= 1)");	
						validRowUB = false;
					}
					if (this.getOrigDim1() >= 0  && tempRowUB > this.getOrigDim1()){ 
						LOG.info(this.printInfoLocation() + "upper-bound row index " + identifierName + " initialized to "  + tempRowUB + " May cause runtime exception (Rows in " + this.getName() + ": " + this.getOrigDim1() +")");
						validRowUB = false;
					}	
					if (isConst_rowLowerBound && tempRowUB < rowLB_1){
						LOG.info(this.printInfoLocation() + "upper-bound row index " 
								+ identifierName + " initialized to " +  tempRowUB 
								+ ", which is greater than lower-bound row index value " 
								+ rowLB_1 + " May cause runtime exception");
						validRowUB = false;
					}

					if (validRowUB) {
						if (constValue instanceof IntIdentifier) {
							_rowUpperBound = new IntIdentifier((IntIdentifier) constValue, constValue);
						} else {
							_rowUpperBound = new DoubleIdentifier((DoubleIdentifier) constValue, constValue);
						}
						isConst_rowUpperBound = true;
					}
				} // end else -- if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
			} // end if (currConstVars.containsKey(identifierName)){
			
		} // end constant propagation for row UB	
		
		
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// PROCESS COLUMN LOWER BOUND
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		// process column lower bound
		if (_colLowerBound instanceof ConstIdentifier && ( _colLowerBound instanceof IntIdentifier || _colLowerBound instanceof DoubleIdentifier )){
			
			Long colLB_1_1 = -1L;
			if (_colLowerBound instanceof IntIdentifier) 
				colLB_1_1 = ((IntIdentifier)_colLowerBound).getValue();
			else 
				colLB_1_1 = UtilFunctions.toLong(((DoubleIdentifier)_colLowerBound).getValue());
				
			if (colLB_1_1 < 1){
				raiseValidateError("lower-bound column index " + colLB_1_1 + " initialized to out of bounds value. Value must be >= 1", conditional);
			}
			if ((this.getOrigDim2() >= 0)  && (colLB_1_1 > this.getOrigDim2())){ 
				raiseValidateError("lower-bound column index " + colLB_1_1 + " initialized to out of bounds value.  Columns in " + this.getName() + ": " + this.getOrigDim2(), conditional);
			}
			// valid lower row bound value
			isConst_colLowerBound = true;
		}
			
		else if (_colLowerBound instanceof ConstIdentifier && !getDataType().isList()) {
			raiseValidateError("assign lower-bound column index for Indexed Identifier " + this.toString() + " the non-numeric value " + _colLowerBound.toString(), conditional);
		}
		
		// perform constant propogation for column lower bound
		else if (_colLowerBound != null && _colLowerBound instanceof DataIdentifier && !(_colLowerBound instanceof IndexedIdentifier)) {
			String identifierName = ((DataIdentifier)_colLowerBound).getName();
			if (currConstVars.containsKey(identifierName) && !conditional){
				ConstIdentifier constValue = currConstVars.get(identifierName);
				
				if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
					LOG.info(this.printInfoLocation() + "attempted to assign lower column bound for " + this.toString() + "the non-numeric value " + constValue.getOutput().toString() + " assigned to " + identifierName + ". May cause runtime exception ");
	
				else {
					
					// test constant propogation			
					long tempColLB = -1L;
					boolean validColLB = true;
					if (constValue instanceof IntIdentifier) 
						tempColLB = ((IntIdentifier)constValue).getValue();
					else
						tempColLB = UtilFunctions.toLong(((DoubleIdentifier)constValue).getValue());
							
					if (tempColLB < 1){
						LOG.info(this.printInfoLocation() + "lower-bound column index " + identifierName + " initialized to "  + tempColLB + " May cause runtime exception (runtime value must be >= 1)");	
						validColLB = false;
					}
					if (this.getOrigDim2() >= 0  && tempColLB > this.getOrigDim2()){ 
						LOG.info(this.printInfoLocation() + "lower-bound column index " 
								+ identifierName + " initialized to " + tempColLB 
								+ " May cause runtime exception (Columns in " + this.getName() 
								+ ": " + this.getOrigDim2() +")");
						validColLB = false;
					}	

					if (validColLB) {
						if (constValue instanceof IntIdentifier) {
							_colLowerBound = new IntIdentifier((IntIdentifier) constValue, constValue);
						} else {
							_colLowerBound = new DoubleIdentifier((DoubleIdentifier) constValue, constValue);
						}
						isConst_colLowerBound = true;
					}
				} // end else -- if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
			} 	
		} // end constant propogation for column LB
		
		// check 1 < indexed column lower-bound < columns in IndexedIdentifier 
		// (assuming column dims available for upper bound)
		Long colLB_1 = -1L;
		if (isConst_colLowerBound) {
				
			if (_colLowerBound instanceof IntIdentifier) 
				colLB_1 = ((IntIdentifier)_colLowerBound).getValue();
			else
				colLB_1 = UtilFunctions.toLong(((DoubleIdentifier)_colLowerBound).getValue());
		}
			
		
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// PROCESS ROW UPPER BOUND
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (_colUpperBound instanceof ConstIdentifier && ( _colUpperBound instanceof IntIdentifier || _colUpperBound instanceof DoubleIdentifier )){
			
			Long colUB_1_1 = -1L;
			if (_colUpperBound instanceof IntIdentifier) 
				colUB_1_1 = ((IntIdentifier)_colUpperBound).getValue();
			else 
				colUB_1_1 = UtilFunctions.toLong(((DoubleIdentifier)_colUpperBound).getValue());
				
			if (colUB_1_1 < 1){
				raiseValidateError("upper-bound column index " + colUB_1_1 + " out of bounds value. Value must be >= 1", conditional);
			}
			if ((this.getOrigDim2() >= 0)  && (colUB_1_1 > this.getOrigDim2())) {
				raiseValidateError("upper-bound column index " + colUB_1_1 + " out of bounds value.  Columns in " + this.getName() + ": " + this.getOrigDim2(), conditional);
			}
			if (isConst_rowLowerBound && colUB_1_1 < colLB_1) {
				raiseValidateError("upper-bound column index " + colUB_1_1 + " greater than lower-bound row index " + colLB_1, conditional);
			}
		    	
			isConst_colUpperBound = true;
		}	
		else if (_colUpperBound instanceof ConstIdentifier && !getDataType().isList()){	
			raiseValidateError("assign upper-bound column index for " + this.toString() + " the non-numeric value " + _colUpperBound.toString(), conditional);
		}
		
		// perform constant propogation for upper row index
		else if (_colUpperBound != null && _colUpperBound instanceof DataIdentifier 
				&& !(_colUpperBound instanceof IndexedIdentifier)) {
			String identifierName = ((DataIdentifier)_colUpperBound).getName();
			
			
			if (currConstVars.containsKey(identifierName) && !conditional){
				ConstIdentifier constValue = currConstVars.get(identifierName);
				
				if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
					LOG.info(this.printInfoLocation() + "attempted to assign upper column bound for " 
							+ this.toString() + "the non-numeric value " + constValue.getOutput().toString() 
							+ " assigned to " + identifierName + ". May cause runtime exception ");
	
				else {						
					// test constant propogation			
					long tempColUB = -1L;
					boolean validColUB = true;
					if (constValue instanceof IntIdentifier) 
						tempColUB = ((IntIdentifier)constValue).getValue();
					else
						tempColUB = UtilFunctions.toLong(((DoubleIdentifier)constValue).getValue());
							
					if (tempColUB < 1){
						LOG.info(this.printInfoLocation() + "upper-bound column index " 
								+ identifierName + " initialized to "  + tempColUB 
								+ " May cause runtime exception (runtime value must be >= 1)");	
						validColUB = false;
					}
					if (this.getOrigDim2() >= 0  && tempColUB > this.getOrigDim2()){ 
						LOG.info(this.printInfoLocation() + "upper-bound column index " 
								+ identifierName + " initialized to "  + tempColUB 
								+ " May cause runtime exception (Columns in " + this.getName() 
								+ ": " + this.getOrigDim2() + ")");
						validColUB = false;
					}	
					if (isConst_colLowerBound && tempColUB < colLB_1){
						LOG.info(this.printInfoLocation() + "upper-bound column index " 
					+ identifierName + " initialized to " +  tempColUB 
					+ ", which is greater than lower-bound column index value " + colLB_1 
					+ " May cause runtime exception");
						validColUB = false;
					}

					if (validColUB) {
						if (constValue instanceof IntIdentifier) {
							_colUpperBound = new IntIdentifier((IntIdentifier) constValue, constValue);
						} else {
							_colUpperBound = new DoubleIdentifier((DoubleIdentifier) constValue, constValue);
						}
						isConst_colUpperBound = true;
					}
				} // end else -- if (!(constValue instanceof IntIdentifier || constValue instanceof DoubleIdentifier ))
			}	
		} // end constant propagation for column UB	
		
		///////////////////////////////////////////////////////////////////////
		// STEP 2: update row dimensions
		///////////////////////////////////////////////////////////////////////
			
		// CASE:  lower == upper --> updated row dim = 1
		if (_rowLowerEqualsUpper) 
			updatedRowDim = 1;
		
		// CASE: (lower == null && upper == null) --> updated row dim = (rows of input)
		else if (_rowLowerBound == null && _rowUpperBound == null){
			updatedRowDim = this.getOrigDim1(); 
		}
		
		// CASE: (lower == null) && (upper == constant) --> updated row dim = (constant)
		else if (_rowLowerBound == null && isConst_rowUpperBound) {
			if (_rowUpperBound instanceof IntIdentifier)
				updatedRowDim = ((IntIdentifier)_rowUpperBound).getValue();
			else if (_rowUpperBound instanceof DoubleIdentifier)
				updatedRowDim = UtilFunctions.toLong(((DoubleIdentifier)_rowUpperBound).getValue());
		}
			
		// CASE: (lower == constant) && (upper == null) && (dimIndex > 0) --> rowCount - lower bound + 1
		else if (isConst_rowLowerBound && _rowUpperBound == null && this.getOrigDim1() >= 0) {
			long rowCount = this.getOrigDim1();
			if (_rowLowerBound instanceof IntIdentifier)
				updatedRowDim = rowCount - ((IntIdentifier)_rowLowerBound).getValue() + 1;
			else if (_rowLowerBound instanceof DoubleIdentifier)
				updatedRowDim = UtilFunctions.toLong(rowCount - ((DoubleIdentifier)_rowLowerBound).getValue() + 1);
		}
		// CASE: (lower == constant) && (upper == constant) --> upper bound - lower bound + 1
		else if (isConst_rowLowerBound && isConst_rowUpperBound) {
			if (_rowLowerBound instanceof IntIdentifier && _rowUpperBound instanceof IntIdentifier)
				updatedRowDim = ((IntIdentifier)_rowUpperBound).getValue() - ((IntIdentifier)_rowLowerBound).getValue() + 1;
			
			else if (_rowLowerBound instanceof DoubleIdentifier && _rowUpperBound instanceof DoubleIdentifier)
				updatedRowDim = UtilFunctions.toLong( ((DoubleIdentifier)_rowUpperBound).getValue() - ((DoubleIdentifier)_rowLowerBound).getValue() + 1);
			
			else if (_rowLowerBound instanceof IntIdentifier && _rowUpperBound instanceof DoubleIdentifier)
				updatedRowDim = UtilFunctions.toLong( ((DoubleIdentifier)_rowUpperBound).getValue() - ((IntIdentifier)_rowLowerBound).getValue() + 1);
			
			else if (_rowLowerBound instanceof DoubleIdentifier && _rowUpperBound instanceof IntIdentifier)
				updatedRowDim = UtilFunctions.toLong( ((IntIdentifier)_rowUpperBound).getValue() - ((DoubleIdentifier)_rowLowerBound).getValue() + 1);
			
		}
		
		// CASE: row dimension is unknown --> assign -1
		else{ 
			updatedRowDim = -1;
		}
		
		
		//////////////////////////////////////////////////////////////////////
		// STEP 3: update column dimensions
		///////////////////////////////////////////////////////////////////////
			
		// CASE:  lower == upper --> updated col dim = 1
		if (_colLowerEqualsUpper) 
			updatedColDim = 1;
		
		// CASE: (lower == null && upper == null) --> updated col dim = (cols of input)
		else if (_colLowerBound == null && _colUpperBound == null){
			updatedColDim = this.getOrigDim2(); 
		}
		// CASE: (lower == null) && (upper == constant) --> updated col dim = (constant)
		else if (_colLowerBound == null && isConst_colUpperBound) {
			if (_colUpperBound instanceof IntIdentifier)
				updatedColDim = ((IntIdentifier)_colUpperBound).getValue();
			else if (_colUpperBound instanceof DoubleIdentifier)
				updatedColDim = UtilFunctions.toLong(((DoubleIdentifier)_colUpperBound).getValue());
		}
			
		// CASE: (lower == constant) && (upper == null) && (dimIndex > 0) --> colCount - lower bound + 1
		else if (isConst_colLowerBound && _colUpperBound == null && this.getOrigDim2() >= 0) {
			long colCount = this.getOrigDim2();
			if (_colLowerBound instanceof IntIdentifier)
				updatedColDim = colCount - ((IntIdentifier)_colLowerBound).getValue() + 1;
			else if (_colLowerBound instanceof DoubleIdentifier)
				updatedColDim = UtilFunctions.toLong(colCount - ((DoubleIdentifier)_colLowerBound).getValue() + 1);
		}
		
		// CASE: (lower == constant) && (upper == constant) --> upper bound - lower bound + 1
		else if (isConst_colLowerBound && isConst_colUpperBound) {
			if (_colLowerBound instanceof IntIdentifier && _colUpperBound instanceof IntIdentifier)
				updatedColDim = ((IntIdentifier)_colUpperBound).getValue() - ((IntIdentifier)_colLowerBound).getValue() + 1;
			
			else if (_colLowerBound instanceof DoubleIdentifier && _colUpperBound instanceof DoubleIdentifier)
				updatedColDim = UtilFunctions.toLong( ((DoubleIdentifier)_colUpperBound).getValue() - ((DoubleIdentifier)_colLowerBound).getValue() + 1);
			
			else if (_colLowerBound instanceof IntIdentifier && _colUpperBound instanceof DoubleIdentifier)
				updatedColDim = UtilFunctions.toLong( ((DoubleIdentifier)_colUpperBound).getValue() - ((IntIdentifier)_colLowerBound).getValue() + 1);
			
			else if (_colLowerBound instanceof DoubleIdentifier && _colUpperBound instanceof IntIdentifier)
				updatedColDim = UtilFunctions.toLong( ((IntIdentifier)_colUpperBound).getValue() - ((DoubleIdentifier)_colLowerBound).getValue() + 1);
			
		}
		
		// CASE: column dimension is unknown --> assign -1
		else{ 
			updatedColDim = -1;
		}
		
		return new IndexPair(updatedRowDim, updatedColDim);
		
	}
	
	public void setOriginalDimensions(long passedDim1, long passedDim2){
		this._origDim1 = passedDim1;
		this._origDim2 = passedDim2;
	}
	
	public long getOrigDim1() { return this._origDim1; }
	public long getOrigDim2() { return this._origDim2; }
	
	
	@Override
	public Expression rewriteExpression(String prefix) {
		IndexedIdentifier newIndexedIdentifier = new IndexedIdentifier(this.getName(), this._rowLowerEqualsUpper, this._colLowerEqualsUpper);
		newIndexedIdentifier.setParseInfo(this);
		
		// set dimensionality information and other Identifier specific properties for new IndexedIdentifier
		newIndexedIdentifier.setProperties(this);
		newIndexedIdentifier.setOriginalDimensions(this._origDim1, this._origDim2);
		
		// set remaining properties (specific to DataIdentifier)
		newIndexedIdentifier._name = prefix + this._name;
		newIndexedIdentifier._valueTypeString = this.getValueType().toString();	
		
		// creates rewritten expression (deep copy)
		newIndexedIdentifier._rowLowerBound = (_rowLowerBound == null) ? null : _rowLowerBound.rewriteExpression(prefix);
		newIndexedIdentifier._rowUpperBound = (_rowUpperBound == null) ? null : _rowUpperBound.rewriteExpression(prefix);
		newIndexedIdentifier._colLowerBound = (_colLowerBound == null) ? null : _colLowerBound.rewriteExpression(prefix);
		newIndexedIdentifier._colUpperBound = (_colUpperBound == null) ? null : _colUpperBound.rewriteExpression(prefix);
		
		return newIndexedIdentifier;
	}
		
	public void setIndices(ArrayList<ArrayList<Expression>> passed) {
		if (passed.size() != 2)
			raiseValidateError("matrix indices must be specified for 2 dimensions");
		
		ArrayList<Expression> rowIndices = passed.get(0);
		ArrayList<Expression> colIndices = passed.get(1);
	
		// case: both upper and lower are defined
		if (rowIndices.size() == 2){			
			_rowLowerBound = rowIndices.get(0);
			_rowUpperBound = rowIndices.get(1);
		}
		// case: only one index is defined --> thus lower = upper
		else if (rowIndices.size() == 1){
			_rowLowerBound = rowIndices.get(0);
			_rowUpperBound = rowIndices.get(0);
			_rowLowerEqualsUpper = true;
		}
		else {
			raiseValidateError("row indices are length " + rowIndices.size() + " -- should be either 1 or 2");
		}
		
		// case: both upper and lower are defined
		if (colIndices.size() == 2){			
			_colLowerBound = colIndices.get(0);
			_colUpperBound = colIndices.get(1);
		}
		// case: only one index is defined --> thus lower = upper
		else if (colIndices.size() == 1){
			_colLowerBound = colIndices.get(0);
			_colUpperBound = colIndices.get(0);
			_colLowerEqualsUpper = true;
		}
		else {
			raiseValidateError("column indices are length " + + colIndices.size() + " -- should be either 1 or 2");
		}
		
		if (_rowLowerBound instanceof FunctionCallIdentifier || _rowUpperBound instanceof FunctionCallIdentifier
				|| _colLowerBound instanceof FunctionCallIdentifier || _colUpperBound instanceof FunctionCallIdentifier){
			raiseValidateError("UDF functions not supported for row or column indices");
		}
		
	}
	
	public Expression getRowLowerBound(){ return this._rowLowerBound; }
	public Expression getRowUpperBound(){ return this._rowUpperBound; }
	public Expression getColLowerBound(){ return this._colLowerBound; }
	public Expression getColUpperBound(){ return this._colUpperBound; }
	
	public void setRowLowerBound(Expression passed){ this._rowLowerBound = passed; }
	public void setRowUpperBound(Expression passed){ this._rowUpperBound = passed; }
	public void setColLowerBound(Expression passed){ this._colLowerBound = passed; }
	public void setColUpperBound(Expression passed){ this._colUpperBound = passed; }
	
	@Override	
	public String toString() {
		String retVal = getName();
		if (_rowLowerBound != null || _rowUpperBound != null || _colLowerBound != null || _colUpperBound != null) {
			retVal += "[";
			
			if (_rowLowerBound != null && _rowUpperBound != null){
				if (_rowLowerBound.toString().equals(_rowUpperBound.toString()))
					retVal += _rowLowerBound.toString();
				else 
					retVal += _rowLowerBound.toString() + ":" + _rowUpperBound.toString();
			}
			else {
				if (_rowLowerBound != null || _rowUpperBound != null){
					if (_rowLowerBound != null)
						retVal += _rowLowerBound.toString();
					
					retVal += ":";
					
					if (_rowUpperBound != null)
						retVal += _rowUpperBound.toString();
				}
			}
				
			retVal += ",";
			
			if (_colLowerBound != null && _colUpperBound != null){
				if (_colLowerBound.toString().equals(_colUpperBound.toString()))
					retVal += _colLowerBound.toString();
				else
					retVal += _colLowerBound.toString() + ":" + _colUpperBound.toString();
			}
			else {
				if (_colLowerBound != null || _colUpperBound != null) {
					
					if (_colLowerBound != null)
						retVal += _colLowerBound.toString();
					
					retVal += ":";
					
					if (_colUpperBound != null)
						retVal += _colUpperBound.toString();
				}
			}
			
			
			retVal += "]";
		}
		return retVal;
	}

	@Override
	// handles case when IndexedIdentifier is on RHS for assignment 
	public VariableSet variablesRead() {
		VariableSet result = new VariableSet();
		
		// add variable being indexed to read set
		result.addVariable(this.getName(), this);
		
		// add variables for indexing expressions
		if (_rowLowerBound != null)
			result.addVariables(_rowLowerBound.variablesRead());
		if (_rowUpperBound != null)
			result.addVariables(_rowUpperBound.variablesRead());
		if (_colLowerBound != null)
			result.addVariables(_colLowerBound.variablesRead());
		if (_colUpperBound != null)
			result.addVariables(_colUpperBound.variablesRead());
		
		return result;
	}
	
	@Override
	public void setProperties(Identifier i){
		_dataType = i.getDataType();
		_valueType = i.getValueType();
		_dim1 = i.getDim1();
		_dim2 = i.getDim2();
		_blocksize = i.getBlocksize();
		_nnz = i.getNnz();
		_format = i.getFileFormat();
		
		if (i instanceof IndexedIdentifier){
			_origDim1 = ((IndexedIdentifier)i).getOrigDim1();
			_origDim2 = ((IndexedIdentifier)i).getOrigDim2();
		}
		else{
			_origDim1 = i.getDim1();
			_origDim2 = i.getDim2();
		}
	}
	
	@Override
	public boolean multipleReturns() {
		return false;
	}
	
} // end class
	
class IndexPair {
	
	public long _row, _col;
	
	public IndexPair (long row, long col){
		_row = row;
		_col = col;
	}
} // end class
