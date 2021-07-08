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

package org.apache.sysds.runtime.matrix.data;

/**
 * Helper class for external key/value exchange.
 * 
 */
public class IJV
{
	private int _i = -1;
	private int _j = -1;
	private double _v = 0;
	
	public IJV() {
		//do nothing
	}

	public IJV set(int i, int j, double v) {
		_i = i;
		_j = j;
		_v = v;
		return this;
	}
	
	public int getI() {
		return _i;
	}
	
	public int getJ() {
		return _j;
	}
	
	public double getV() {
		return _v;
	}
	
	public boolean onDiag() {
		return _i == _j;
	}
	
	@Override
	public String toString() {
		return "("+_i+", "+_j+"): "+_v;
	}
}
