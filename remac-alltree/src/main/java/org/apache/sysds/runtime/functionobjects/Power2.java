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

// Singleton class

public class Power2 extends ValueFunction 
{
	private static final long serialVersionUID = -4370611388912121328L;

	private static Power2 singleObj = null;
	
	private Power2() {
		// nothing to do here
	}
	
	public static Power2 getPower2FnObject() {
		if ( singleObj == null )
			singleObj = new Power2();
		return singleObj;
	}
	
	@Override
	public double execute(double in1) {
		return in1*in1; //ignore in2 because always 2; 
	}
	
	@Override
	public double execute(double in1, double in2) {
		return in1*in1; //ignore in2 because always 2; 
	}

	@Override
	public double execute(long in1, long in2) {
		//for robustness regarding long overflows (only used for scalar instructions)
		double dval = ((double)in1 * in2);
		if( dval > Long.MAX_VALUE )
			return dval;	
		
		return in1*in1; //ignore in2 because always 2;
	}

}
