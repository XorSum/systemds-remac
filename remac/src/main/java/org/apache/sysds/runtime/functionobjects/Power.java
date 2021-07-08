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

public class Power extends ValueFunction 
{

	private static final long serialVersionUID = 1503063846565177447L;

	private static Power singleObj = null;
	
	private Power() {
		// nothing to do here
	}
	
	public static Power getPowerFnObject() {
		if ( singleObj == null )
			singleObj = new Power();
		return singleObj;
	}
	
	@Override
	public double execute(double in1, double in2) {
		return Math.pow(in1, in2); 
	}

	@Override
	public double execute(long in1, long in2) {
		return Math.pow(in1, in2);
	}

}
