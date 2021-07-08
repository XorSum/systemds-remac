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

package org.apache.sysds.runtime;

import org.apache.sysds.api.DMLException;

/**
 * This exception should be thrown to flag runtime errors -- DML equivalent to java.lang.RuntimeException.
 */
public class DMLRuntimeException extends DMLException 
{
	private static final long serialVersionUID = 1L;

	public DMLRuntimeException(String string) {
		super(string);
	}
	
	public DMLRuntimeException(Exception e) {
		super(e);
	}

	public DMLRuntimeException(String string, Exception ex){
		super(string,ex);
	}
}
