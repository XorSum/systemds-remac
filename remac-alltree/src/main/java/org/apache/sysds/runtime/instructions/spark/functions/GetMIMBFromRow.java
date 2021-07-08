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

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

import scala.Tuple2;

public class GetMIMBFromRow implements PairFunction<Row, MatrixIndexes, MatrixBlock> {

	private static final long serialVersionUID = 2291741087248847581L;

	@Override	
	public Tuple2<MatrixIndexes, MatrixBlock> call(Row row) throws Exception {
			MatrixIndexes indx = (MatrixIndexes) row.apply(0);
			MatrixBlock blk = (MatrixBlock) row.apply(1);
			return new Tuple2<>(indx, blk);
	}

}
