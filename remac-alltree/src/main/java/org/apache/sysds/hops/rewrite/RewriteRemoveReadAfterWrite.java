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

package org.apache.sysds.hops.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.sysds.common.Types.OpOpData;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.parser.DataExpression;

/**
 * Rule: RemoveReadAfterWrite. If there is a persistent read with the same filename
 * as a persistent write, and read has a higher line number than the write,
 * we remove the read and consume the write input directly. This is important for two
 * reasons (1) correctness and (2) performance. Without this rewrite, we could not
 * guarantee the order of read-after-write because there is not data dependency
 * 
 */
public class RewriteRemoveReadAfterWrite extends HopRewriteRule
{
	
	@Override
	@SuppressWarnings("unchecked")
	public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state)
	{
		if( roots == null )
			return null;
		
		//collect all persistent reads and writes
		HashMap<String,Hop> reads = new HashMap<>();
		HashMap<String,Hop> writes = new HashMap<>();
		for( Hop h : roots ) 
			collectPersistentReadWriteOps( h, writes, reads );
		
		Hop.resetVisitStatus(roots);
		
		//check persistent reads for read-after-write pattern
		for( Entry<String, Hop> e : reads.entrySet() )
		{
			String rfname = e.getKey();
			Hop rhop = e.getValue();
			if( writes.containsKey(rfname)  //same persistent filename
				&&   (writes.get(rfname).getBeginLine()<rhop.getBeginLine() //read after write
				   || writes.get(rfname).getEndLine()<rhop.getEndLine()) ) //note: account for bug in line handling, TODO remove after line handling resolved
			{
				//rewire read consumers to write input
				Hop input = writes.get(rfname).getInput().get(0);
				ArrayList<Hop> parents = (ArrayList<Hop>) rhop.getParent().clone();
				for( Hop p : parents )
					HopRewriteUtils.replaceChildReference(p, rhop, input);
			}
		}
		
		return roots;
	}

	@Override
	public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
		//do noting, read/write do not occur in predicates
		return root;
	}

	private void collectPersistentReadWriteOps(Hop hop, HashMap<String,Hop> pWrites, HashMap<String,Hop> pReads) {
		if( hop.isVisited() )
			return;
		
		//process childs
		if( !hop.getInput().isEmpty() )
			for( Hop c : hop.getInput() )
				collectPersistentReadWriteOps(c, pWrites, pReads);
		
		//process current hop
		if( hop instanceof DataOp )
		{
			DataOp dop = (DataOp)hop;
			if( dop.getOp()==OpOpData.PERSISTENTREAD )
				pReads.put(dop.getFileName(), dop);
			else if( dop.getOp()==OpOpData.PERSISTENTWRITE )
			{
				Hop fname = dop.getInput().get(dop.getParameterIndex(DataExpression.IO_FILENAME));
				if( fname instanceof LiteralOp ) //only constant writes
					pWrites.put(((LiteralOp) fname).getStringValue(), dop);	
			}
		}
		
		hop.setVisited();
	}
}
