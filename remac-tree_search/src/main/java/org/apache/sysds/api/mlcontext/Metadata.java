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

package org.apache.sysds.api.mlcontext;

import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;

/**
 * Abstract metadata class for MLContext API. Complex types such as SystemDS
 * matrices and frames typically require metadata, so this abstract class serves
 * as a common parent class of these types.
 *
 */
public abstract class Metadata {

	protected Long numRows = null;
	protected Long numColumns = null;
	protected Integer blockSize = null;
	protected Long numNonZeros = null;

	/**
	 * Convert the metadata to a DataCharacteristics object. If all field
	 * values are {@code null}, {@code null} is returned.
	 *
	 * @return the metadata as a DataCharacteristics object, or {@code null}
	 *         if all field values are null
	 */
	public MatrixCharacteristics asMatrixCharacteristics() {

		if (numRows == null && numColumns == null && blockSize == null && numNonZeros == null) {
			return null;
		}

		long nr = (numRows == null) ? -1 : numRows;
		long nc = (numColumns == null) ? -1 : numColumns;
		int blen = (blockSize == null) ? ConfigurationManager.getBlocksize() : blockSize;
		long nnz = (numNonZeros == null) ? -1 : numNonZeros;
		return new MatrixCharacteristics(nr, nc, blen, nnz);
	}

	protected String fieldDisplay(Object field) {
		if (field == null)
			return "None";
		return field.toString();
	}

	/**
	 * Obtain the number of columns
	 *
	 * @return the number of columns
	 */
	public Long getNumColumns() {
		return numColumns;
	}

	/**
	 * Obtain the number of non-zero values
	 *
	 * @return the number of non-zero values
	 */
	public Long getNumNonZeros() {
		return numNonZeros;
	}

	/**
	 * Obtain the number of rows
	 *
	 * @return the number of rows
	 */
	public Long getNumRows() {
		return numRows;
	}

	/**
	 * Obtain the number of rows/cols per block
	 *
	 * @return the number of rows/cols per block
	 */
	public Integer getBlocksize() {
		return blockSize;
	}

	/**
	 * Set the metadata fields based on a DataCharacteristics object.
	 *
	 * @param matrixCharacteristics
	 *            the matrix metadata as a DataCharacteristics object
	 */
	public void setMatrixCharacteristics(MatrixCharacteristics matrixCharacteristics) {
		this.numRows = matrixCharacteristics.getRows();
		this.numColumns = matrixCharacteristics.getCols();
		this.numNonZeros = matrixCharacteristics.getNonZeros();
		this.blockSize = matrixCharacteristics.getBlocksize();
	}

	/**
	 * Set the number of columns
	 *
	 * @param numColumns
	 *            the number of columns
	 */
	public void setNumColumns(Long numColumns) {
		this.numColumns = numColumns;
	}

	/**
	 * Set the number of non-zero values
	 *
	 * @param numNonZeros
	 *            the number of non-zero values
	 */
	public void setNumNonZeros(Long numNonZeros) {
		this.numNonZeros = numNonZeros;
	}

	/**
	 * Set the number of rows
	 *
	 * @param numRows
	 *            the number of rows
	 */
	public void setNumRows(Long numRows) {
		this.numRows = numRows;
	}

	/**
	 * Set the number of rows per block
	 *
	 * @param blen
	 *            the number of rows/cols per block
	 */
	public void setBlocksize(Integer blen) {
		this.blockSize = blen;
	}

	@Override
	public String toString() {
		return "rows: " + fieldDisplay(numRows) + ", columns: " + fieldDisplay(numColumns) + ", non-zeros: "
			+ fieldDisplay(numNonZeros) + ", blocksize: " + fieldDisplay(blockSize);
	}
}
