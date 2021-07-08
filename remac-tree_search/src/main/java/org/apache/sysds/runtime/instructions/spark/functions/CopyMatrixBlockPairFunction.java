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

package org.apache.sysds.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.sysds.lops.Checkpoint;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import scala.Tuple2;

import java.util.Iterator;

/**
 * General purpose copy function for binary block rdds. This function can be used in
 * mapToPair (copy matrix indexes and blocks). It supports both deep and shallow copies 
 * of key/value pairs.
 * 
 */
public class CopyMatrixBlockPairFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>,MatrixIndexes, MatrixBlock>
{
	private static final long serialVersionUID = -196553327495233360L;

	private boolean _deepCopy = true;
	
	public CopyMatrixBlockPairFunction() {
		this(true);
	}
	
	public CopyMatrixBlockPairFunction(boolean deepCopy) {
		_deepCopy = deepCopy;
	}

	@Override
	public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> arg0) 
		throws Exception 
	{	
		return new CopyBlockPairIterator(arg0);
	}
	
	private class CopyBlockPairIterator extends LazyIterableIterator<Tuple2<MatrixIndexes,MatrixBlock>>
	{
		public CopyBlockPairIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> iter) {
			super(iter);
		}
		
		@Override
		protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg) 
			throws Exception 
		{
			if( _deepCopy ) {
				MatrixIndexes ix = new MatrixIndexes(arg._1());
				MatrixBlock block = null;
				//always create deep copies in more memory-efficient CSR representation 
				//if block is already in sparse format
				if( Checkpoint.CHECKPOINT_SPARSE_CSR && arg._2.isInSparseFormat() )
					block = new MatrixBlock(arg._2, SparseBlock.Type.CSR, true);
				else
					block = new MatrixBlock(arg._2());
				return new Tuple2<>(ix,block);
			}
			else {
				return arg;
			}
		}
	}
}