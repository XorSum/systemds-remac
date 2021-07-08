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

package org.apache.sysds.runtime.functionobjects;

import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.KahanObject;


public class Mean extends ValueFunction 
{
	private static final long serialVersionUID = 1967222020396371269L;

	private static Mean singleObj = null;
	
	private KahanPlus _plus = null; 
	
	private Mean() {
		_plus = KahanPlus.getKahanPlusFnObject();
	}
	
	public static Mean getMeanFnObject() {
		if ( singleObj == null )
			singleObj = new Mean();
		return singleObj;
	}
	
	@Override
	public Data execute(Data in1, double in2, double count) {
		KahanObject kahanObj=(KahanObject)in1;
		double delta = (in2-kahanObj._sum)/count;
		_plus.execute(in1, delta);	
		return kahanObj;
	}
	
	/**
	 * Simplified version of execute(Data in1, double in2) 
	 * without exception handling and casts.
	 * 
	 * @param in1 Kahan object input
	 * @param in2 double input
	 * @param count the count to divide by
	 */
	public void execute2(KahanObject in1, double in2, double count) 
	{
		double delta = (in2-in1._sum)/count;
		_plus.execute2(in1, delta);
	}
}
