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

import org.apache.sysds.runtime.controlprogram.ProgramBlock;
import org.apache.sysds.runtime.instructions.Instruction;

public class OptTreePlanMappingRuntime extends OptTreePlanMapping
{
	private Map<Long, Object> _id_rtprog;

	public OptTreePlanMappingRuntime() {
		super();
		_id_rtprog = new HashMap<>();
	}
	
	public long putMapping( Instruction inst, OptNode n ) {
		long id = _idSeq.getNextID();
		_id_rtprog.put(id, inst);
		_id_optnode.put(id, n);
		n.setID(id);
		return id;
	}
	
	public long putMapping( ProgramBlock pb, OptNode n ) {
		long id = _idSeq.getNextID();
		_id_rtprog.put(id, pb);
		_id_optnode.put(id, n);
		n.setID(id);
		return id;
	}
	
	public void replaceMapping( ProgramBlock pb, OptNode n ) {
		long id = n.getID();
		_id_rtprog.put(id, pb);
		_id_optnode.put(id, n);
	}
	
	public Object getMappedObject( long id ) {
		return _id_rtprog.get( id );
	}

	@Override
	public void clear() {
		super.clear();
		_id_rtprog.clear();
	}
}
