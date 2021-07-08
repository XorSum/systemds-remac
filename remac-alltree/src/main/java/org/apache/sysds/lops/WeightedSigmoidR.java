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

package org.apache.sysds.lops;

 
import org.apache.sysds.lops.LopProperties.ExecType;
import org.apache.sysds.lops.WeightedSigmoid.WSigmoidType;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;

public class WeightedSigmoidR extends Lop 
{
	public static final String OPCODE = "redwsigmoid";
	
	private WSigmoidType _wsType = null;

	private boolean _cacheU = false;
	private boolean _cacheV = false;
	
	public WeightedSigmoidR(Lop input1, Lop input2, Lop input3, DataType dt, ValueType vt, WSigmoidType wt, boolean cacheU, boolean cacheV, ExecType et) {
		super(Lop.Type.WeightedSigmoid, dt, vt);
		addInput(input1); //X
		addInput(input2); //U
		addInput(input3); //V
		input1.addOutput(this);
		input2.addOutput(this);
		input3.addOutput(this);
		
		//setup mapmult parameters
		_wsType = wt;
		_cacheU = cacheU;
		_cacheV = cacheV;
		setupLopProperties(et);
	}
	
	@Override
	public String toString() {
		return "Operation = WeightedSigmoidR";
	}

	@Override
	public String getInstructions(String input1, String input2, String input3, String output) {
		return InstructionUtils.concatOperands(
			getExecType().name(),
			OPCODE,
			getInputs().get(0).prepInputOperand(input1),
			getInputs().get(1).prepInputOperand(input2),
			getInputs().get(2).prepInputOperand(input3),
			prepOutputOperand(output),
			_wsType.name(),
			String.valueOf(_cacheU),
			String.valueOf(_cacheV));
	}
}
