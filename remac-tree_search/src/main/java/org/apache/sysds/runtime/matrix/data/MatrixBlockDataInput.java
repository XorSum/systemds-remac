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

package org.apache.sysds.runtime.matrix.data;

import java.io.IOException;

import org.apache.sysds.runtime.data.SparseBlock;

/**
 * Any data input that is intended to support fast deserialization / read
 * of entire blocks should implement this interface. On read of a matrix block
 * we check if the input stream is an implementation of this interface, if
 * yes we let the implementation directly pass the entire block instead of value-by-value.
 * 
 * Known implementation classes:
 *    - FastBufferedDataInputStream
 *    - CacheDataInput
 *    
 */
public interface MatrixBlockDataInput 
{	
	/**
	 * Reads the double array from the data input into the given dense block
	 * and returns the number of non-zeros. 
	 * 
	 * @param len ?
	 * @param varr ?
	 * @return number of non-zeros
	 * @throws IOException if IOException occurs
	 */
	public long readDoubleArray(int len, double[] varr) 
		throws IOException;
	
	/**
	 * Reads the sparse rows array from the data input into a sparse block
	 * and returns the number of non-zeros.
	 * 
	 * @param rlen number of rows
	 * @param nnz number of non-zeros
	 * @param rows sparse block
	 * @return number of non-zeros
	 * @throws IOException if IOExcepton occurs
	 */
	public long readSparseRows(int rlen, long nnz, SparseBlock rows) 
		throws IOException;
}
