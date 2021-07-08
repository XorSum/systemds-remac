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

package org.apache.sysds.runtime.instructions.spark.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.FastBufferedDataInputStream;
import org.apache.sysds.runtime.util.FastBufferedDataOutputStream;


public class CorrMatrixBlock implements Externalizable
{

	private static final long serialVersionUID = -2204456681697015083L;
	
	private MatrixBlock _value = null;
	private MatrixBlock _corr = null;
	
	public CorrMatrixBlock() {
		//do nothing (required for Externalizable)
	}
	
	public CorrMatrixBlock( MatrixBlock value ) {
		//shallow copy of passed value
		_value = value;
	}
	
	public CorrMatrixBlock( MatrixBlock value, MatrixBlock corr ) {
		//shallow copy of passed value and corr
		_value = value;
		_corr = corr;
	}
	
	public MatrixBlock getValue(){
		return _value;
	}
	
	public MatrixBlock getCorrection(){
		return _corr;
	}
	
	public CorrMatrixBlock set(MatrixBlock value, MatrixBlock corr) {
		_value = value;
		_corr = corr;
		return this;
	}
	
	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient deserialization. 
	 * 
	 * @param is object input
	 * @throws IOException if IOException occurs
	 */
	@Override
	public void readExternal(ObjectInput is) 
		throws IOException
	{
		DataInput dis = is;
		
		if( is instanceof ObjectInputStream ) {
			//fast deserialize of dense/sparse blocks
			ObjectInputStream ois = (ObjectInputStream)is;
			dis = new FastBufferedDataInputStream(ois);
		}
		
		readHeaderAndPayload(dis);
	}

	/**
	 * Redirects the default java serialization via externalizable to our default 
	 * hadoop writable serialization for efficient serialization. 
	 * 
	 * @param os object output
	 * @throws IOException if IOException occurs
	 */
	@Override
	public void writeExternal(ObjectOutput os) 
		throws IOException
	{
		if( os instanceof ObjectOutputStream && !_value.isEmptyBlock(false) ) {
			//fast serialize of dense/sparse blocks
			ObjectOutputStream oos = (ObjectOutputStream)os;
			FastBufferedDataOutputStream fos = new FastBufferedDataOutputStream(oos);
			writeHeaderAndPayload(fos);
			fos.flush();
		}
		else {
			//default serialize (general case)
			writeHeaderAndPayload(os);	
		}
	}

	private void writeHeaderAndPayload(DataOutput dos) 
		throws IOException 
	{
		boolean writeCorr = (_corr != null
			&& !_corr.isEmptyBlock(false));
		dos.writeByte(writeCorr ? 1 : 0);
		_value.write(dos);
		if( writeCorr )
			_corr.write(dos);
	}

	private void readHeaderAndPayload(DataInput dis) 
		throws IOException 
	{
		boolean corrExists = (dis.readByte() != 0) ? true : false;
		_value = new MatrixBlock();
		_value.readFields(dis);
		if( corrExists ) {
			_corr = new MatrixBlock();
			_corr.readFields(dis);	
		}
	}
}
