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

package org.apache.sysds.runtime.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

/**
 * Custom record reader for binary block. Currently its only purpose is to allow for
 * detailed profiling of overall read time (io, deserialize, decompress).
 * 
 * NOTE: not used by default.
 */
public class BinaryBlockRecordReader extends SequenceFileRecordReader<MatrixIndexes,MatrixBlock>
{
	//private long _time = 0;
	
	public BinaryBlockRecordReader(Configuration conf, FileSplit split)
		throws IOException 
	{
		super(conf, split);
		
	}
	
	@Override
	public synchronized boolean next(MatrixIndexes key, MatrixBlock value)
		throws IOException 
	{
		//long t0 = System.nanoTime();		
		boolean ret = super.next(key, value);		
		//long t1 = System.nanoTime();
		
		//_time+=(t1-t0);
		
		return ret;
	}

	@Override
	public synchronized void close() 
		throws IOException 
	{		
		//in milliseconds.
		//System.out.println(_time/1000000);
		super.close();
	}
}
