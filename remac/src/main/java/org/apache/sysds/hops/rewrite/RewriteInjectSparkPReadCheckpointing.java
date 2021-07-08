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

import org.apache.sysds.common.Types.OpOpData;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.OptimizerUtils;

/**
 * Rule: BlockSizeAndReblock. For all statement blocks, determine
 * "optimal" block size, and place reblock Hops. For now, we just
 * use BlockSize 1K x 1K and do reblock after Persistent Reads and
 * before Persistent Writes.
 */
public class RewriteInjectSparkPReadCheckpointing extends HopRewriteRule
{
	@Override
	public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
		if(  !OptimizerUtils.isSparkExecutionMode()  ) 
			return roots;
		
		if( roots == null )
			return null;

		//top-level hops never modified
		for( Hop h : roots ) 
			rInjectCheckpointAfterPRead(h);
		
		return roots;
	}

	@Override
	public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
		//not applicable to predicates (we do not allow persistent reads there)
		return root;
	}

	private void rInjectCheckpointAfterPRead( Hop hop ) 
	{
		if(hop.isVisited())
			return;
		
		// The reblocking is performed after transform, and hence checkpoint only non-transformed reads.
		if( (hop instanceof DataOp && ((DataOp)hop).getOp()==OpOpData.PERSISTENTREAD)
			|| hop.requiresReblock() )
		{
			//make given hop for checkpointing (w/ default storage level)
			//note: we do not recursively process childs here in order to prevent unnecessary checkpoints
			hop.setRequiresCheckpoint(true);
		}
		else
		{
			//process childs
			if( hop.getInput() != null ) {
				//process all childs (prevent concurrent modification by index access)
				for( int i=0; i<hop.getInput().size(); i++ )
					rInjectCheckpointAfterPRead( hop.getInput().get(i) );
			}
		}
		
		hop.setVisited();
	}
}
