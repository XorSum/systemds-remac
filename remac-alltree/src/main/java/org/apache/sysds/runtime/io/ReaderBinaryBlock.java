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

package org.apache.sysds.runtime.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.HDFSTool;

public class ReaderBinaryBlock extends MatrixReader
{
	protected boolean _localFS = false;
	
	public ReaderBinaryBlock( boolean localFS )
	{
		_localFS = localFS;
	}
	
	public void setLocalFS(boolean flag) {
		_localFS = flag;
	}
	
	@Override
	public MatrixBlock readMatrixFromHDFS(String fname, long rlen, long clen, int blen, long estnnz) 
		throws IOException, DMLRuntimeException 
	{
		//early abort for known empty matrices (e.g., remote parfor result vars)
		if( RETURN_EMPTY_NNZ0 && estnnz == 0 )
			return new MatrixBlock((int)rlen, (int)clen, true);
		
		//allocate output matrix block
		MatrixBlock ret = createOutputMatrixBlock(rlen, clen, blen, estnnz, false, false);
		
		//prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());	
		Path path = new Path( (_localFS ? "file:///" : "") + fname); 
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
		
		//check existence and non-empty file
		checkValidInputFile(fs, path); 
	
		//core read 
		readBinaryBlockMatrixFromHDFS(path, job, fs, ret, rlen, clen, blen);
		
		//finally check if change of sparse/dense block representation required
		if( !AGGREGATE_BLOCK_NNZ )
			ret.recomputeNonZeros();
		ret.examSparsity();
		
		return ret;
	}
	
	@Override
	public MatrixBlock readMatrixFromInputStream(InputStream is, long rlen, long clen, int blen, long estnnz) 
		throws IOException, DMLRuntimeException 
	{
		throw new DMLRuntimeException("Not implemented yet.");
	}

	public ArrayList<IndexedMatrixValue> readIndexedMatrixBlocksFromHDFS(String fname, long rlen, long clen, int blen) 
		throws IOException, DMLRuntimeException 
	{
		//allocate output matrix block collection
		ArrayList<IndexedMatrixValue> ret = new ArrayList<>();
		
		//prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path( (_localFS ? "file:///" : "") + fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
		
		//check existence and non-empty file
		checkValidInputFile(fs, path);
	
		//core read 
		readBinaryBlockMatrixBlocksFromHDFS(path, job, fs, ret, rlen, clen, blen);
		
		return ret;
	}
	
	protected static MatrixBlock getReuseBlock(int blen, boolean sparse) {
		//note: we allocate the reuse block in CSR because this avoids unnecessary
		//reallocations in the presence of a mix of sparse and ultra-sparse blocks,
		//where ultra-sparse deserialization only reuses CSR blocks
		MatrixBlock value = new MatrixBlock(blen, blen, sparse);
		if( sparse ) {
			value.allocateAndResetSparseBlock(true, SparseBlock.Type.CSR);
			value.getSparseBlock().allocate(0, blen*blen);
		}
		return value;
	}

	
	/**
	 * Note: For efficiency, we directly use SequenceFile.Reader instead of SequenceFileInputFormat-
	 * InputSplits-RecordReader (SequenceFileRecordReader). First, this has no drawbacks since the
	 * SequenceFileRecordReader internally uses SequenceFile.Reader as well. Second, it is 
	 * advantageous if the actual sequence files are larger than the file splits created by   
	 * informat.getSplits (which is usually aligned to the HDFS block size) because then there is 
	 * overhead for finding the actual split between our 1k-1k blocks. This case happens
	 * if the read matrix was create by CP or when jobs directly write to large output files 
	 * (e.g., parfor matrix partitioning).
	 * 
	 * @param path file path
	 * @param job job configuration
	 * @param fs file system
	 * @param dest matrix block
	 * @param rlen number of rows
	 * @param clen number of columns
	 * @param blen number of rows in block
	 * @param blen number of columns in block
	 * @throws IOException if IOException occurs
	 */
	private static void readBinaryBlockMatrixFromHDFS( Path path, JobConf job, FileSystem fs, MatrixBlock dest, long rlen, long clen, int blen )
		throws IOException
	{
		boolean sparse = dest.isInSparseFormat();
		MatrixIndexes key = new MatrixIndexes(); 
		MatrixBlock value = getReuseBlock(blen, sparse);
		long lnnz = 0; //aggregate block nnz
		
		//set up preferred custom serialization framework for binary block format
		if( HDFSTool.USE_BINARYBLOCK_SERIALIZATION )
			HDFSTool.addBinaryBlockSerializationFramework( job );
		
		for( Path lpath : IOUtilFunctions.getSequenceFilePaths(fs, path) ) //1..N files 
		{
			//directly read from sequence files (individual partfiles)
			SequenceFile.Reader reader = new SequenceFile
				.Reader(job, SequenceFile.Reader.file(lpath));
			
			try
			{
				//note: next(key, value) does not yet exploit the given serialization classes, record reader does but is generally slower.
				while( reader.next(key, value) )
				{	
					//empty block filter (skip entire block)
					if( value.isEmptyBlock(false) )
						continue;
					
					int row_offset = (int)(key.getRowIndex()-1)*blen;
					int col_offset = (int)(key.getColumnIndex()-1)*blen;
					
					int rows = value.getNumRows();
					int cols = value.getNumColumns();
					
					//bound check per block
					if( row_offset + rows < 0 || row_offset + rows > rlen || col_offset + cols<0 || col_offset + cols > clen )
					{
						throw new IOException("Matrix block ["+(row_offset+1)+":"+(row_offset+rows)+","+(col_offset+1)+":"+(col_offset+cols)+"] " +
								              "out of overall matrix range [1:"+rlen+",1:"+clen+"].");
					}
			
					//copy block to result
					if( sparse )
					{
						//note: append requires final sort (but prevents repeated shifting)
						dest.appendToSparse(value, row_offset, col_offset);
					} 
					else
					{
						dest.copy( row_offset, row_offset+rows-1, 
								   col_offset, col_offset+cols-1,
								   value, false );
					}
					
					//maintain nnz as aggregate of block nnz
					lnnz += value.getNonZeros();
				}
			}
			finally
			{
				IOUtilFunctions.closeSilently(reader);
			}
		}
		
		//post-processing
		dest.setNonZeros( lnnz );
		if( sparse && clen>blen ){
			//no need to sort if 1 column block since always sorted
			dest.sortSparseRows();
		}
	}
	
	private static void readBinaryBlockMatrixBlocksFromHDFS( Path path, JobConf job, FileSystem fs, Collection<IndexedMatrixValue> dest, long rlen, long clen, int blen )
		throws IOException
	{
		MatrixIndexes key = new MatrixIndexes(); 
		MatrixBlock value = new MatrixBlock();
			
		//set up preferred custom serialization framework for binary block format
		if( HDFSTool.USE_BINARYBLOCK_SERIALIZATION )
			HDFSTool.addBinaryBlockSerializationFramework( job );
		
		for( Path lpath : IOUtilFunctions.getSequenceFilePaths(fs, path) ) //1..N files 
		{
			//directly read from sequence files (individual partfiles)
			SequenceFile.Reader reader = new SequenceFile
				.Reader(job, SequenceFile.Reader.file(lpath));
			
			try
			{
				while( reader.next(key, value) )
				{	
					int row_offset = (int)(key.getRowIndex()-1)*blen;
					int col_offset = (int)(key.getColumnIndex()-1)*blen;
					int rows = value.getNumRows();
					int cols = value.getNumColumns();
					
					//bound check per block
					if( row_offset + rows < 0 || row_offset + rows > rlen || col_offset + cols<0 || col_offset + cols > clen )
					{
						throw new IOException("Matrix block ["+(row_offset+1)+":"+(row_offset+rows)+","+(col_offset+1)+":"+(col_offset+cols)+"] " +
								              "out of overall matrix range [1:"+rlen+",1:"+clen+"].");
					}
			
					//copy block to result
					dest.add(new IndexedMatrixValue(new MatrixIndexes(key), new MatrixBlock(value)));
				}
			}
			finally
			{
				IOUtilFunctions.closeSilently(reader);
			}
		}
	}
}
