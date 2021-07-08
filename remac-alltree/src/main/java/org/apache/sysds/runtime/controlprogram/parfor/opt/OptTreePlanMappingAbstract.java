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

package org.apache.sysds.runtime.controlprogram.parfor.opt;

import java.util.HashMap;
import java.util.Map;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.parser.DMLProgram;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.runtime.controlprogram.ProgramBlock;

public class OptTreePlanMappingAbstract extends OptTreePlanMapping
{
	private DMLProgram _prog;
	private Program _rtprog;
	private Map<Long, Object> _id_hlprog;
	private Map<Long, Object> _id_rtprog;
	
	public OptTreePlanMappingAbstract( ) {
		super();
		_prog = null;
		_rtprog = null;
		_id_hlprog = new HashMap<>();
		_id_rtprog = new HashMap<>();
	}
	
	public void putRootProgram( DMLProgram prog, Program rtprog ) {
		_prog = prog;
		_rtprog = rtprog;
	}
	
	public long putHopMapping( Hop hops, OptNode n ) {
		long id = _idSeq.getNextID();
		_id_hlprog.put(id, hops);
		_id_rtprog.put(id, null);
		_id_optnode.put(id, n);	
		n.setID(id);
		return id;
	}
	
	public long putProgMapping( StatementBlock sb, ProgramBlock pb, OptNode n ) {
		long id = _idSeq.getNextID();
		_id_hlprog.put(id, sb);
		_id_rtprog.put(id, pb);
		_id_optnode.put(id, n);
		n.setID(id);
		return id;
	}
	
	public Object[] getRootProgram() {
		Object[] ret = new Object[2];
		ret[0] = _prog;
		ret[1] = _rtprog;
		return ret;
	}
	
	public Hop getMappedHop( long id ) {
		return (Hop)_id_hlprog.get( id );
	}
	
	public Object[] getMappedProg( long id ) {
		Object[] ret = new Object[2];
		ret[0] = _id_hlprog.get(id);
		ret[1] = _id_rtprog.get(id);
		return ret;
	}
	
	public ProgramBlock getMappedProgramBlock(long id) {
		return (ProgramBlock) _id_rtprog.get(id);
	}
	
	public void replaceMapping( ProgramBlock pb, OptNode n ) {
		long id = n.getID();
		_id_rtprog.put(id, pb);
		_id_optnode.put(id, n);
	}
	
	@Override
	public void clear() {
		super.clear();
		_prog = null;
		_rtprog = null;
		_id_hlprog.clear();
		_id_rtprog.clear();
	}
}
