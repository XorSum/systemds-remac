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

package org.apache.sysds.runtime.instructions.gpu;

import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.operators.Operator;

public abstract class RelationalBinaryGPUInstruction extends GPUInstruction {

	protected CPOperand _input1;
	protected CPOperand _input2;
	protected CPOperand _output;

	protected RelationalBinaryGPUInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode,
			String istr) {
		super(op, opcode, istr);
		_gputype = GPUINSTRUCTION_TYPE.RelationalBinary;
		_input1 = in1;
		_input2 = in2;
		_output = out;
	}

	public static RelationalBinaryGPUInstruction parseInstruction ( String str ) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields ( parts, 3 );

		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);

		DataType dt1 = in1.getDataType();
		DataType dt2 = in2.getDataType();
		DataType dt3 = out.getDataType();

		Operator operator = (dt1 != dt2) ?
				InstructionUtils.parseScalarBinaryOperator(opcode, (dt1 == DataType.SCALAR)) :
				InstructionUtils.parseBinaryOperator(opcode);

		if(dt1 == DataType.MATRIX && dt2 == DataType.MATRIX && dt3 == DataType.MATRIX) {
			return new MatrixMatrixRelationalBinaryGPUInstruction(operator, in1, in2, out, opcode, str);
		}
		else if( dt3 == DataType.MATRIX && ((dt1 == DataType.SCALAR && dt2 == DataType.MATRIX) || (dt1 == DataType.MATRIX && dt2 == DataType.SCALAR)) ) {
			return new ScalarMatrixRelationalBinaryGPUInstruction(operator, in1, in2, out, opcode, str);
		}
		else
			throw new DMLRuntimeException("Unsupported GPU RelationalBinaryGPUInstruction.");
	}
}
