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

package org.apache.sysds.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import scala.Tuple2;

public class MatrixAppendRSPInstruction extends AppendRSPInstruction {

	protected MatrixAppendRSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, boolean cbind,
			String opcode, String istr) {
		super(op, in1, in2, out, cbind, opcode, istr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// reduce-only append (output must have at most one column block)
		SparkExecutionContext sec = (SparkExecutionContext)ec;
		checkBinaryAppendInputCharacteristics(sec, _cbind, true, false);
		
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryMatrixBlockRDDHandleForVariable( input1.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> in2 = sec.getBinaryMatrixBlockRDDHandleForVariable( input2.getName() );
		
		//execute reduce-append operations (partitioning preserving)
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = in1
			.join(in2)
			.mapValues(new ReduceSideAppendFunction(_cbind));

		//put output RDD handle into symbol table
		updateBinaryAppendOutputDataCharacteristics(sec, _cbind);
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(output.getName(), input1.getName());
		sec.addLineageRDD(output.getName(), input2.getName());
	}

	private static class ReduceSideAppendFunction implements Function<Tuple2<MatrixBlock, MatrixBlock>, MatrixBlock> 
	{
		private static final long serialVersionUID = -6763904972560309095L;

		private boolean _cbind = true;
		
		public ReduceSideAppendFunction(boolean cbind) {
			_cbind = cbind;
		}
		
		@Override
		public MatrixBlock call(Tuple2<MatrixBlock, MatrixBlock> arg0)
			throws Exception 
		{
			return arg0._1().append(arg0._2(), new MatrixBlock(), _cbind);
		}
	}
}
