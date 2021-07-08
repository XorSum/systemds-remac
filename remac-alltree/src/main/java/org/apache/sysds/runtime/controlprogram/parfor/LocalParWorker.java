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

package org.apache.sysds.runtime.controlprogram.parfor;

import java.util.Collection;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.conf.CompilerConfig;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Stat;
import org.apache.sysds.runtime.controlprogram.parfor.stat.StatisticMonitor;
import org.apache.sysds.runtime.controlprogram.parfor.stat.Timing;

/**
 * Instances of this class can be used to execute tasks in parallel. Within each ParWorker 
 * multiple iterations of a single task and subsequent tasks are executed sequentially.
 * 
 * Resiliency approach: retry on computation error, abort on task queue error
 * 
 * 
 */
public class LocalParWorker extends ParWorker implements Runnable
{
	protected final LocalTaskQueue<Task> _taskQueue;
	protected final CompilerConfig _cconf;
	protected final boolean _stopped;
	protected final int _max_retry;
	protected Collection<String> _fnNames = null;
	
	public LocalParWorker( long ID, LocalTaskQueue<Task> q, ParForBody body, CompilerConfig cconf, int max_retry, boolean monitor ) {
		super(ID, body, monitor);
		_taskQueue = q;
		_cconf = cconf;
		_stopped   = false;
		_max_retry = max_retry;
	}

	public void setFunctionNames(Collection<String> fnNames) {
		_fnNames = fnNames;
	}
	
	public Collection<String> getFunctionNames() {
		return _fnNames;
	}
	
	@Override
	public void run() 
	{
		// monitoring start
		Timing time1 = ( _monitor ? new Timing(true) : null ); 
		
		//setup fair scheduler pool for worker thread, but avoid unnecessary
		//spark context creation (if data cached already created)
		int pool = -1;
		if( OptimizerUtils.isSparkExecutionMode() 
			&& SparkExecutionContext.isSparkContextCreated() ) {
			SparkExecutionContext sec = (SparkExecutionContext)_ec;
			pool = sec.setThreadLocalSchedulerPool();
		}

		// Initialize this GPUContext to this thread
		if (DMLScript.USE_ACCELERATOR) {
			try {
				_ec.getGPUContext(0).initializeThread();
			} catch(DMLRuntimeException e) {
				LOG.error("Error executing task because of failure in GPU backend: ",e);
				LOG.error("Stopping LocalParWorker.");
				return;
			}
		}
		
		//setup compiler config for worker thread
		ConfigurationManager.setLocalConfig(_cconf);
		
		// continuous execution (execute tasks until (1) stopped or (2) no more tasks)
		Task lTask = null; 
		try {
			while( !_stopped ) {
				//dequeue the next task (abort on NO_MORE_TASKS or error)
				try {
					lTask = _taskQueue.dequeueTask();
					
					if( lTask == LocalTaskQueue.NO_MORE_TASKS ) // task queue closed (no more tasks)
						break; //normal end of parallel worker
				}
				catch(Exception ex) {
					// abort on taskqueue error
					LOG.warn("Error reading from task queue: "+ex.getMessage());
					LOG.warn("Stopping LocalParWorker.");
					break; //no exception thrown to prevent blocking on join
				}
				
				//execute the task sequentially (re-try on error)
				boolean success = false;
				int retrys = _max_retry;
				
				while( !success ) {
					try {
						///////
						//core execution (see ParWorker)
						executeTask( lTask );
						success = true;
					} 
					catch (Exception ex)  {
						LOG.error("Failed to execute "+lTask.toString()+", retry:"+retrys, ex);
						
						if( retrys > 0 )
							retrys--; //retry on task error
						else {
							// abort on no remaining retrys
							LOG.error("Error executing task: ",ex);
							LOG.error("Stopping LocalParWorker.");
							break; //no exception thrown to prevent blocking on join 
						}
					}
				}
			}
		}
		finally {
			//cleanup fair scheduler pool for worker thread
			if( OptimizerUtils.isSparkExecutionMode() && pool != -1 ) {
				SparkExecutionContext sec = (SparkExecutionContext)_ec;
				sec.cleanupThreadLocalSchedulerPool(pool);
			}
		}
		
		if( _monitor ) {
			StatisticMonitor.putPWStat(_workerID, Stat.PARWRK_NUMTASKS, _numTasks);
			StatisticMonitor.putPWStat(_workerID, Stat.PARWRK_NUMITERS, _numIters);
			StatisticMonitor.putPWStat(_workerID, Stat.PARWRK_EXEC_T, time1.stop());
		}
	}
}
