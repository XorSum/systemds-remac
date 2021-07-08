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

import java.util.LinkedList;
import java.util.List;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.ParForProgramBlock;
import org.apache.sysds.runtime.controlprogram.parfor.Task.TaskType;
import org.apache.sysds.runtime.instructions.cp.IntObject;

/**
 * This factoring task partitioner virtually iterates over the given FOR loop (from, to, incr),
 * creates iterations and group them to tasks. Note that the task size is used here.
 * The tasks are created with decreasing size for good load balance of heterogeneous tasks.
 * 
 * 
 * See the original paper for details:
 * [Susan Flynn Hummel, Edith Schonberg, Lawrence E. Flynn: 
 * Factoring: a practical and robust method for scheduling parallel loops. 
 * SC 1991: 610-632]
 * 
 */
public class TaskPartitionerFactoring extends TaskPartitioner
{
	
	private int _numThreads = -1;
	
	public TaskPartitionerFactoring( long taskSize, int numThreads, String iterVarName, IntObject fromVal, IntObject toVal, IntObject incrVal ) 
	{
		super(taskSize, iterVarName, fromVal, toVal, incrVal);
		
		_numThreads = numThreads;
	}

	@Override
	public List<Task> createTasks()
	{
		LinkedList<Task> tasks = new LinkedList<>();
		long lFrom = _fromVal.getLongValue();
		long lTo = _toVal.getLongValue();
		long lIncr = _incrVal.getLongValue();
		int P = _numThreads;  // number of parallel workers
		long N = _numIter;    // total number of iterations
		long R = N;           // remaining number of iterations
		long K = -1;          // next _numThreads task sizes
		TaskType type = null; // type of iterations: range tasks (similar to run-length encoding) make only sense if taskSize>3
		
		for( long i = lFrom; i<=lTo;  )
		{
			K = determineNextBatchSize(R, P);
			R -= (K * P);
			
			type = (ParForProgramBlock.USE_RANGE_TASKS_IF_USEFUL && K>3 ) ? 
				TaskType.RANGE : TaskType.SET;
			
			//for each logical processor
			for( int j=0; j<P; j++ )
			{
				if( i > lTo ) //no more iterations
					break;
				
				//create new task and add to list of tasks
				Task lTask = new Task(_iterVarName, type);
				tasks.addLast(lTask);
				
				// add iterations to task 
				if( type == TaskType.SET ) {
					//value based tasks
					for( long k=0; k<K && i<=lTo; k++, i+=lIncr )
						lTask.addIteration(new IntObject(i));
				}
				else {
					//determine end of task
					long to = Math.min( i+(K-1)*lIncr, lTo );
					
					//range based tasks
					lTask.addIteration(new IntObject(i));     //from
					lTask.addIteration(new IntObject(to));    //to
					lTask.addIteration(new IntObject(lIncr)); //increment
					i = to + lIncr;
				}
			}
		}

		return tasks;
	}

	@Override
	public long createTasks(LocalTaskQueue<Task> queue) 
	{
		long numCreatedTasks = 0;
		
		long lFrom  = _fromVal.getLongValue();
		long lTo    = _toVal.getLongValue();
		long lIncr  = _incrVal.getLongValue();
		
		int P = _numThreads;   // number of parallel workers
		long N = _numIter;     // total number of iterations
		long R = N;            // remaining number of iterations
		long K = -1;           //next _numThreads task sizes	
	    TaskType type = null;  // type of iterations: range tasks (similar to run-length encoding) make only sense if taskSize>3
		
		try
		{
			for( long i = lFrom; i<=lTo;  )
			{
				K = determineNextBatchSize(R, P);
				R -= (K * P);
				
				type = (ParForProgramBlock.USE_RANGE_TASKS_IF_USEFUL && K>3 ) ? 
						   TaskType.RANGE : TaskType.SET;
				
				//for each logical processor
				for( int j=0; j<P; j++ )
				{
					if( i > lTo ) //no more iterations
						break;
					
					//create new task and add to list of tasks
					Task lTask = new Task(_iterVarName, type);
					
					// add iterations to task 
					if( type == TaskType.SET ) 
					{
						//value based tasks
						for( long k=0; k<K && i<=lTo; k++, i+=lIncr )
							lTask.addIteration(new IntObject(i));
					}
					else 
					{
						//determine end of task
						long to = Math.min( i+(K-1)*lIncr, lTo );
						
						//range based tasks
						lTask.addIteration(new IntObject(i));	    //from
						lTask.addIteration(new IntObject(to));    //to
						lTask.addIteration(new IntObject(lIncr));	//increment
						
						i = to + lIncr;
					}
					
					//add task to queue (after all iteration added for preventing raise conditions)
					queue.enqueueTask( lTask );
					numCreatedTasks++;
				}
			}
			
			// mark end of task input stream
			queue.closeInput();	
		}
		catch(Exception ex)
		{
			throw new DMLRuntimeException(ex);
		}
	
		return numCreatedTasks;
	}
	
	
	/**
	 * Computes the task size (number of iterations per task) for the next numThreads tasks 
	 * given the number of remaining iterations R, and the number of Threads.
	 * 
	 * NOTE: x can be set to different values, but the original paper argues for x=2.
	 * 
	 * @param R ?
	 * @param P ?
	 * @return next batch task size
	 */
	protected long determineNextBatchSize(long R, int P) 
	{
		int x = 2;
		long K = (long) Math.ceil((double)R / ( x * P )); //NOTE: round creates more tasks
		
		if( K < 1 ) //account for rounding errors
			K = 1;
		
		return K;
	}
	
}
