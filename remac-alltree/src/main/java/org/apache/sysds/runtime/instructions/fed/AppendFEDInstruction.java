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

package org.apache.sysds.runtime.instructions.fed;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederationMap.FType;
import org.apache.sysds.runtime.controlprogram.federated.FederationUtils;
import org.apache.sysds.runtime.functionobjects.OffsetColumnIndex;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;

public class AppendFEDInstruction extends BinaryFEDInstruction {
	protected boolean _cbind; //otherwise rbind
	
	protected AppendFEDInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out,
		boolean cbind, String opcode, String istr) {
		super(FEDType.Append, op, in1, in2, out, opcode, istr);
		_cbind = cbind;
	}
	
	public static AppendFEDInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 5, 4);
		
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[parts.length - 2]);
		boolean cbind = Boolean.parseBoolean(parts[parts.length - 1]);
		
		Operator op = new ReorgOperator(OffsetColumnIndex.getOffsetColumnIndexFnObject(-1));
		return new AppendFEDInstruction(op, in1, in2, out, cbind, opcode, str);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		//get inputs
		MatrixObject mo1 = ec.getMatrixObject(input1.getName());
		MatrixObject mo2 = ec.getMatrixObject(input2.getName());
		DataCharacteristics dc1 = mo1.getDataCharacteristics();
		DataCharacteristics dc2 = mo1.getDataCharacteristics();
		
		//check input dimensions
		if (_cbind && mo1.getNumRows() != mo2.getNumRows()) {
			throw new DMLRuntimeException(
				"Append-cbind is not possible for federated input matrices " + input1.getName() + " and "
				+ input2.getName() + " with different number of rows: " + mo1.getNumRows() + " vs "
				+ mo2.getNumRows());
		}
		else if (!_cbind && mo1.getNumColumns() != mo2.getNumColumns()) {
			throw new DMLRuntimeException(
				"Append-rbind is not possible for federated input matrices " + input1.getName() + " and "
				+ input2.getName() + " with different number of columns: " + mo1.getNumColumns()
				+ " vs " + mo2.getNumColumns());
		}
		
		if( mo1.isFederated(FType.ROW) && _cbind ) {
			FederatedRequest fr1 = mo1.getFedMapping().broadcast(mo2);
			FederatedRequest fr2 = FederationUtils.callInstruction(instString, output,
				new CPOperand[]{input1, input2}, new long[]{mo1.getFedMapping().getID(), fr1.getID()});
			mo1.getFedMapping().execute(getTID(), true, fr1, fr2);
			//derive new fed mapping for output
			MatrixObject out = ec.getMatrixObject(output);
			out.getDataCharacteristics().set(dc1.getRows(), dc1.getCols()+dc2.getCols(),
				dc1.getBlocksize(), dc1.getNonZeros()+dc2.getNonZeros());
			out.setFedMapping(mo1.getFedMapping().copyWithNewID(fr2.getID()));
		}
		else if( mo1.isFederated(FType.ROW) && mo2.isFederated(FType.ROW) && !_cbind ) {
			MatrixObject out = ec.getMatrixObject(output);
			out.getDataCharacteristics().set(dc1.getRows()+dc2.getRows(), dc1.getCols(),
				dc1.getBlocksize(), dc1.getNonZeros()+dc2.getNonZeros());
			long id = FederationUtils.getNextFedDataID();
			out.setFedMapping(mo1.getFedMapping().copyWithNewID(id).rbind(dc1.getRows(), mo2.getFedMapping()));
		}
		else { //other combinations
			throw new DMLRuntimeException("Federated AggregateBinary not supported with the "
				+ "following federated objects: "+mo1.isFederated()+" "+mo2.isFederated());
		}
	}
}
