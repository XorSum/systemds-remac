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

package org.apache.sysds.runtime.transform.encode;

import java.util.ArrayList;
import java.util.List;

import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.matrix.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.IndexRange;
import org.apache.sysds.runtime.util.UtilFunctions;

/**
 * Simple composite encoder that applies a list of encoders 
 * in specified order. By implementing the default encoder API
 * it can be used as a drop-in replacement for any other encoder. 
 * 
 */
public class EncoderPassThrough extends Encoder
{
	private static final long serialVersionUID = -8473768154646831882L;
	
	protected EncoderPassThrough(int[] ptCols, int clen) {
		super(ptCols, clen); //1-based 
	}
	
	public EncoderPassThrough() {
		this(new int[0], 0);
	}

	@Override
	public MatrixBlock encode(FrameBlock in, MatrixBlock out) {
		return apply(in, out);
	}

	@Override
	public void build(FrameBlock in) {
		//do nothing
	}
	
	@Override 
	public MatrixBlock apply(FrameBlock in, MatrixBlock out) {
		for( int j=0; j<_colList.length; j++ ) {
			int col = _colList[j]-1;
			ValueType vt = in.getSchema()[col];
			for( int i=0; i<in.getNumRows(); i++ ) {
				Object val = in.get(i, col);
				out.quickSetValue(i, col, (val==null||(vt==ValueType.STRING 
						&& val.toString().isEmpty())) ? Double.NaN : 
						UtilFunctions.objectToDouble(vt, val));
			}
		}
		
		return out;
	}
	
	@Override
	public Encoder subRangeEncoder(IndexRange ixRange) {
		List<Integer> colList = new ArrayList<>();
		for(int col : _colList) {
			if(col >= ixRange.colStart && col < ixRange.colEnd)
				// add the correct column, removed columns before start
				colList.add((int) (col - (ixRange.colStart - 1)));
		}
		if(colList.isEmpty())
			// empty encoder -> return null
			return null;
		return new EncoderPassThrough(colList.stream().mapToInt(i -> i).toArray(),
			(int) (ixRange.colEnd - ixRange.colStart));
	}
	
	@Override
	public void mergeAt(Encoder other, int row, int col) {
		if(other instanceof EncoderPassThrough) {
			mergeColumnInfo(other, col);
			return;
		}
		super.mergeAt(other, row, col);
	}

	@Override
	public FrameBlock getMetaData(FrameBlock meta) {
		//do nothing
		return meta;
	}
	
	@Override
	public void initMetaData(FrameBlock meta) {
		//do nothing
	}
}
