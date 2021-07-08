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

package org.apache.sysds.runtime.transform.meta;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.sysds.runtime.matrix.data.Pair;

import java.util.TreeMap;

public class TfOffsetMap implements Serializable
{
	private static final long serialVersionUID = 8949124761287236703L;

	private HashMap<Long, Long> _map = null;
	private long _rmRows = -1;
	
	public TfOffsetMap( List<Pair<Long,Long>> rmRows ) {
		//sort input list of <key, rm rows per block>
		TreeMap<Long, Long> tmap = new TreeMap<>();
		for( Pair<Long, Long> pair : rmRows )
			tmap.put(pair.getKey(), pair.getValue());
		
		//compute shifted keys and build hash table
		_map = new HashMap<>();
		long shift = 0;
		for( Entry<Long, Long> e : tmap.entrySet() ) {
			_map.put(e.getKey(), e.getKey()-shift);
			shift += e.getValue();
		}
		_rmRows = shift;
	}

	public long getOffset(long oldKey) {
		return _map.get(oldKey);
	}	

	public long getNumRmRows() {
		return _rmRows;
	}
}
