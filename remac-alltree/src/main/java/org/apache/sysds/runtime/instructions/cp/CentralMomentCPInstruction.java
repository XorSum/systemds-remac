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

package org.apache.sysds.runtime.instructions.cp;

import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.CM;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.CMOperator;
import org.apache.sysds.runtime.matrix.operators.CMOperator.AggregateOperationTypes;

public class CentralMomentCPInstruction extends AggregateUnaryCPInstruction {

	private CentralMomentCPInstruction(CMOperator cm, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
			String opcode, String str) {
		super(cm, in1, in2, in3, out, AUType.DEFAULT, opcode, str);
	}

	public static CentralMomentCPInstruction parseInstruction(String str) {
		CPOperand in1 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		CPOperand in2 = null; 
		CPOperand in3 = null; 
		CPOperand out = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
		
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0]; 
		
		//check supported opcode
		if( !opcode.equalsIgnoreCase("cm") ) {
			throw new DMLRuntimeException("Unsupported opcode "+opcode);
		}
			
		if ( parts.length == 4 ) {
			// Example: CP.cm.mVar0.Var1.mVar2; (without weights)
			in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
			parseUnaryInstruction(str, in1, in2, out);
		}
		else if ( parts.length == 5) {
			// CP.cm.mVar0.mVar1.Var2.mVar3; (with weights)
			in2 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
			in3 = new CPOperand("", ValueType.UNKNOWN, DataType.UNKNOWN);
			parseUnaryInstruction(str, in1, in2, in3, out);
		}
	
		/* 
		 * Exact order of the central moment MAY NOT be known at compilation time.
		 * We first try to parse the second argument as an integer, and if we fail, 
		 * we simply pass -1 so that getCMAggOpType() picks up AggregateOperationTypes.INVALID.
		 * It must be updated at run time in processInstruction() method.
		 */
		
		int cmOrder;
		try {
			if ( in3 == null ) {
				cmOrder = Integer.parseInt(in2.getName());
			}
			else {
				cmOrder = Integer.parseInt(in3.getName());
			}
		} catch(NumberFormatException e) {
			cmOrder = -1; // unknown at compilation time
		}
		
		AggregateOperationTypes opType = CMOperator.getCMAggOpType(cmOrder);
		CMOperator cm = new CMOperator(CM.getCMFnObject(opType), opType);
		return new CentralMomentCPInstruction(cm, in1, in2, in3, out, opcode, str);
	}
	
	@Override
	public void processInstruction( ExecutionContext ec ) {
		String output_name = output.getName();

		/*
		 * The "order" of the central moment in the instruction can 
		 * be set to INVALID when the exact value is unknown at 
		 * compilation time. We first need to determine the exact 
		 * order and update the CMOperator, if needed.
		 */
		
		MatrixBlock matBlock = ec.getMatrixInput(input1.getName());

		CPOperand scalarInput = (input3==null ? input2 : input3);
		ScalarObject order = ec.getScalarInput(scalarInput); 
		
		CMOperator cm_op = ((CMOperator)_optr); 
		if ( cm_op.getAggOpType() == AggregateOperationTypes.INVALID )
			cm_op = cm_op.setCMAggOp((int)order.getLongValue());
		
		CM_COV_Object cmobj = null; 
		if (input3 == null ) {
			cmobj = matBlock.cmOperations(cm_op);
		}
		else {
			MatrixBlock wtBlock = ec.getMatrixInput(input2.getName());
			cmobj = matBlock.cmOperations(cm_op, wtBlock);
			ec.releaseMatrixInput(input2.getName());
		}
		
		ec.releaseMatrixInput(input1.getName());
		
		double val = cmobj.getRequiredResult(cm_op);
		ec.setScalarOutput(output_name, new DoubleObject(val));
	}
}
