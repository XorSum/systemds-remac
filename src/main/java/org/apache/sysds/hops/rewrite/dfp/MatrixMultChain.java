package org.apache.sysds.hops.rewrite.dfp;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;


import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.*;
import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;


public class MatrixMultChain {

    Hop originalTree;
    ArrayList<Hop> trees;
    HashMap<Pair<Long,Long>, Triple<Hop,Hop,Long> > allTreeesHashMap;
    // HashMap<hashkey      , Triple<tree,subExp,count>

    public void gao(Hop originTree) {
        this.originalTree = originTree;
        this.allTreeesHashMap = new HashMap<>();

                    long  startTime = System.currentTimeMillis();
        this.trees = BaoLi.generateAllTrees(originTree);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime -startTime;
                    System.out.println(">>构造所有的等价二叉树执行耗时：" + totalTime + " ms");

                    startTime = System.currentTimeMillis();
        for (Hop tree: trees) {
            HashMap<Pair<Long,Long>, ArrayList<Hop> > singleTreeHashMap = new HashMap<>();
//            System.out.println("Stats tree: ");
//            tree.resetVisitStatus();
//            System.out.println(Explain.explain(tree));

            statsAllNode(tree,tree,singleTreeHashMap);

            for (HashMap.Entry<Pair<Long,Long>, ArrayList<Hop> > keyValue: singleTreeHashMap.entrySet()) {
                Pair<Long,Long> key = keyValue.getKey();
                ArrayList<Hop> value = keyValue.getValue();
                boolean shouldUpdate = true;
                if (this.allTreeesHashMap.containsKey(key)) {
                    Triple<Hop,Hop,Long> old =  this.allTreeesHashMap.get(key);
                    if (old.getRight()>=value.size()) {
                        shouldUpdate = false;
                    }
                }
                if (shouldUpdate) {
                    this.allTreeesHashMap.put(key, Triple.of(tree,value.get(0),(long)value.size()) );
                }
            }
        }
            endTime = System.currentTimeMillis();
            totalTime = endTime -startTime;
            System.out.println(">>所有子式插入哈希表执行耗时：" + totalTime + " ms");


    }

    private void statsAllNode(Hop tree, Hop node, HashMap<Pair<Long,Long>, ArrayList<Hop> > singleTree) {
        if (isSampleHop(node)) return;
        Pair<Long, Long> hash = hashHopDag(node);
        if (!singleTree.containsKey(hash)) {
            singleTree.put(hash, new ArrayList<>());
        }
        singleTree.get(hash).add(node);
      //  begin
        Hop tran = HopRewriteUtils.createTranspose(node);
        tran = reorder(tran);
        Pair<Long,Long> thash = hashHopDag(tran);
        if (!singleTree.containsKey(thash)) {
            singleTree.put(thash,new ArrayList<>());
        }
        singleTree.get(thash).add(tran);
      //  end
//        Long treeId = tree.getHopID();
//        Long nodeId = node.getHopID();
//        System.out.println(treeId + " " + nodeId);
        for (int i = 0; i < node.getInput().size(); i++) {
            statsAllNode( tree, node.getInput().get(i),singleTree);
        }
    }

    public  Hop getTree(Pair<Long,Long> targetHash,Hop targetDag) {

        if (!allTreeesHashMap.containsKey(targetHash))
            return this.originalTree;
//        System.out.println("Get Tree");
//        System.out.println("change");
        Triple<Hop,Hop,Long> triple = allTreeesHashMap.get(targetHash);
//        triple.getLeft().resetVisitStatus();
//        System.out.println(Explain.explain(triple.getLeft()));
        Hop tree = deepCopyHopsDag(triple.getLeft());
//        tree.resetVisitStatus();
//        System.out.println(Explain.explain(tree));
        tree = replace(null,tree,targetHash,targetDag);
        return tree;
    }

    private Hop replace(Hop parent,Hop hop,Pair<Long,Long> targetHash,  Hop targetDag ) {
     //   if (hop.isVisited()) return hop;
      //  if ( hashHopDag(hop)==targetHash ) {
        Hop tran = HopRewriteUtils.createTranspose(targetDag);
        if (isSame(hop,targetDag)) {
        //    System.out.println(" = target");
            if (parent!=null)  {
                HopRewriteUtils.replaceChildReference(parent,hop,targetDag);
                HopRewriteUtils.cleanupUnreferenced(hop);
            }
            hop = targetDag;
        } else if (isSame(hop,tran)) {

         //   System.out.println(" = t(target)");
            if (parent!=null)  {
                HopRewriteUtils.replaceChildReference(parent,hop,tran);
                HopRewriteUtils.cleanupUnreferenced(hop);
            }
            hop = tran;
        }
        else  {
            for (int i=0;i<hop.getInput().size();i++) {
                Hop tmp = hop.getInput().get(i);
                tmp = replace(hop,tmp,targetHash,targetDag);
                hop.getInput().set(i,tmp);
            }
        }
        return hop;
    }

}
