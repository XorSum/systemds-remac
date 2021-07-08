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

package org.apache.sysds.runtime.controlprogram;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.ProgramRewriter;
import org.apache.sysds.hops.rewrite.dfp.RewriteLoopConstant;
import org.apache.sysds.hops.rewrite.dfp.alltrees.RewriteAllTres;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.*;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.DMLScriptException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject.UpdateType;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.cp.BooleanObject;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.lineage.LineageDedupUtils;
import org.apache.sysds.utils.Explain;


public class WhileProgramBlock extends ProgramBlock
{
	private ArrayList<Instruction> _predicate;
	private ArrayList<ProgramBlock> _childBlocks;

	protected static final Log LOG = LogFactory.getLog(WhileProgramBlock.class.getName());

	public WhileProgramBlock(Program prog, ArrayList<Instruction> predicate) {
		super(prog);
		_predicate = predicate;
		_childBlocks = new ArrayList<>();
	}

	public void addProgramBlock(ProgramBlock childBlock) {
		_childBlocks.add(childBlock);
	}

	public ArrayList<Instruction> getPredicate() {
		return _predicate;
	}

	public void setPredicate( ArrayList<Instruction> predicate ) {
		_predicate = predicate;
	}

	@Override
	public ArrayList<ProgramBlock> getChildBlocks() {
		return _childBlocks;
	}

	@Override
	public boolean isNested() {
		return true;
	}

	private BooleanObject executePredicate(ExecutionContext ec)
	{
		BooleanObject result = null;
		try
		{
			if( _sb!=null )
			{
				WhileStatementBlock wsb = (WhileStatementBlock)_sb;
				Hop predicateOp = wsb.getPredicateHops();
				boolean recompile = wsb.requiresPredicateRecompilation();
				result = (BooleanObject) executePredicate(_predicate, predicateOp, recompile, ValueType.BOOLEAN, ec);
			}
			else
				result = (BooleanObject) executePredicate(_predicate, null, false, ValueType.BOOLEAN, ec);
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(this.printBlockErrorLocation() + "Failed to evaluate the while predicate.", ex);
		}

		//(guaranteed to be non-null, see executePredicate/getScalarInput)
		return result;
	}

	void rGetLiteral(Hop hop, ArrayList<Long> literals) {
		System.out.println(hop.getHopID()+" "+hop.getName());
		if (hop instanceof LiteralOp) {
			LiteralOp literalOp = (LiteralOp) hop;
			long value =	literalOp.getLongValue();
			if (value>0) literals.add(value);
		}
		for (Hop i: hop.getInput()) {
			rGetLiteral(i,literals);
		}
	}

	long getInerationNumber() {
		WhileStatementBlock wsb = (WhileStatementBlock)_sb;
		Hop predicateOp = wsb.getPredicateHops();
		System.out.println(Explain.explain(predicateOp));
		ArrayList<Long> literals = new ArrayList<>();
		rGetLiteral(predicateOp,literals);
		long max = 2;
		for (long i: literals) {
			max = Math.max(max,i);
		}
		LOG.info("iterationNumber="+max);
		return max;
	}


	@Override
	public void execute(ExecutionContext ec)
	{
		//execute while loop
		try
		{
//			System.out.println( "StatementBlock = " +  _sb);
			long start1 = System.nanoTime();
			try {
			//	System.out.println("Reducer Number: "+ OptimizerUtils.getNumReducers(false));
			    VariableSet variablesUpdated = _sb.variablesUpdated();
			    long iterationNumber = getInerationNumber();
				ProgramRewriter rewriter = new ProgramRewriter(new RewriteLoopConstant(ec,variablesUpdated,iterationNumber));
				ArrayList<StatementBlock> sbs = new ArrayList<>();
				sbs.add(_sb);
				sbs = rewriter.rRewriteStatementBlocks(sbs, new ProgramRewriteStatus(), false);

				LOG.info(RewriteLoopConstant.preLoop);

//				if (RewriteLoopConstant.preLoop.size()>0) {
					for (int i = 0; i  < RewriteLoopConstant.preLoop.size(); i++) {
						LOG.info("execute pre loop statement block");
						LOG.info(Explain.explain(RewriteLoopConstant.preLoop.get(i)));
						DMLProgram dmlProg = new DMLProgram();
						StatementBlock preLoop = RewriteLoopConstant.preLoop.get(i);
						dmlProg.addStatementBlock(preLoop);
						DMLTranslator dmlt = new DMLTranslator(dmlProg);
						dmlt.constructLops(dmlProg);
						Program rtProg = dmlt.getRuntimeProgram(dmlProg, ConfigurationManager.getDMLConfig());
						rtProg.execute(ec);
						LOG.info("pre execute done");
					}
//				}
			} catch (Exception e) {
				LOG.debug("while program optimize error");
				e.printStackTrace();
			}
			long end1 = System.nanoTime();

			LOG.info("all search time = " + (RewriteAllTres.searchTime / 1e9) + "s");
			System.out.println("all search time = " + (RewriteAllTres.searchTime / 1e9) + "s");
			System.out.println("STOP");
			System.exit(0);

//			LOG.info("all generate options time = " + (RewriteCoordinate.allGenerateOptionsTime / 1e9) + "s");
//			System.out.println("all generate options time = " + (RewriteCoordinate.allGenerateOptionsTime / 1e9) + "s");

//			LOG.info("all generate combinations time = " + (RewriteCoordinate.allGenerateCombinationsTime / 1e9) + "s");
//			System.out.println("all generate combinations time = " + (RewriteCoordinate.allGenerateCombinationsTime / 1e9) + "s");
//
//			System.out.println("remac optimize time = "+((end1-start1)/1e9)+"s");
//			LOG.info("remac optimize time = "+((end1-start1)/1e9)+"s");

			// prepare update in-place variables
			UpdateType[] flags = prepareUpdateInPlaceVariables(ec, _tid);

			// compute and store the number of distinct paths
			if (DMLScript.LINEAGE_DEDUP)
				ec.getLineage().initializeDedupBlock(this, ec);

			//run loop body until predicate becomes false
			while( executePredicate(ec).getBooleanValue() ) {
				long start = System.nanoTime();
				TempPersist.createNewFrame();
				if (DMLScript.LINEAGE_DEDUP)
					ec.getLineage().resetDedupPath();

				if (DMLScript.LINEAGE_DEDUP)
					// create a new dedup map, if needed, to trace this iteration
					ec.getLineage().createDedupPatch(this, ec);

				//execute all child blocks
				for (int i=0 ; i < _childBlocks.size() ; i++) {
					_childBlocks.get(i).execute(ec);
				}
				TempPersist.cleanPersistedCses();
				if (DMLScript.LINEAGE_DEDUP) {
					LineageDedupUtils.replaceLineage(ec);
					// hook the dedup map to the main lineage trace
					ec.getLineage().traceCurrentDedupPath(this, ec);
				}
				long end = System.nanoTime();
				System.out.println("WhileExecTime = "+((end-start)/1e9)+" s");
			}

			// clear current LineageDedupBlock
			if (DMLScript.LINEAGE_DEDUP)
				ec.getLineage().clearDedupBlock();

			// reset update-in-place variables
			resetUpdateInPlaceVariableFlags(ec, flags);
		}
		catch (DMLScriptException e) {
			//propagate stop call
			throw e;
		}
		catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating while program block", e);
		}

		//execute exit instructions
		executeExitInstructions(_exitInstruction, "while", ec);
	}

	public void setChildBlocks(ArrayList<ProgramBlock> childs) {
		_childBlocks = childs;
	}

	@Override
	public String printBlockErrorLocation(){
		return "ERROR: Runtime error in while program block generated from while statement block between lines " + _beginLine + " and " + _endLine + " -- ";
	}
}