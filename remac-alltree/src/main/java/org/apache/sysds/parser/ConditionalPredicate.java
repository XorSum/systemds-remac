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

package org.apache.sysds.parser;

public class ConditionalPredicate implements ParseInfo
{
	private Expression _expr;
	
	public ConditionalPredicate(Expression expr){
		_expr = expr;
	}
	
	public Expression getPredicate(){
		return _expr;
	}
	public void setPredicate(Expression expr){
		_expr = expr;
	}
	
	@Override
	public String toString(){
		return _expr.toString();
	}
	
	public VariableSet variablesRead() {
		VariableSet result = new VariableSet();
		result.addVariables(_expr.variablesRead());
	 	return result;
	}

	 
	public VariableSet variablesUpdated() {
		VariableSet result = new VariableSet();
		result.addVariables(_expr.variablesUpdated());
	 	return result;
	}
	
	///////////////////////////////////////////////////////////////////////////
	// store position information for expressions
	///////////////////////////////////////////////////////////////////////////
	private String _filename;
	private int _beginLine, _beginColumn;
	private int _endLine, _endColumn;
	private String _text;
	
	@Override
	public void setFilename(String fname)   { _filename = fname;   }
	@Override
	public void setBeginLine(int passed)    { _beginLine = passed;   }
	@Override
	public void setBeginColumn(int passed) 	{ _beginColumn = passed; }
	@Override
	public void setEndLine(int passed) 		{ _endLine = passed;   }
	@Override
	public void setEndColumn(int passed)	{ _endColumn = passed; }
	@Override
	public void setText(String text) { _text = text; }

	@Override
	public String getFilename()	{ return _filename;   }
	@Override
	public int getBeginLine()	{ return _beginLine;   }
	@Override
	public int getBeginColumn() { return _beginColumn; }
	@Override
	public int getEndLine() 	{ return _endLine;   }
	@Override
	public int getEndColumn()	{ return _endColumn; }
	@Override
	public String getText() { return _text; }
}
