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

package org.apache.sysds.parser.dml;

import java.util.HashMap;

import org.apache.sysds.parser.FunctionDictionary;
import org.apache.sysds.parser.FunctionStatementBlock;
import org.apache.sysds.parser.Statement;

/**
 * This class exists solely to prevent compiler warnings.
 * 
 * <p>
 * The ExpressionInfo and StatementInfo classes are shared among both parsers
 * (R-like and Python-like dialects), and Antlr-generated code assumes that
 * these classes are present in the parser's namespace.
 */

public class StatementInfo {
	public Statement stmt = null;
	
	// Valid only for import statements
	public HashMap<String,FunctionDictionary<FunctionStatementBlock>> namespaces = null;
	
	// Valid only for function statement
	public String functionName = "";
}
