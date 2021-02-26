package org.apache.sysds.hops.rewrite.dfp.alltrees;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteRule;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.RewriteCommonSubexpressionElimination;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMatrixMatrixMultMergeRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMatrixMatrixMultSplitRule;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag;
import org.apache.sysds.utils.Explain;
import org.spark_project.jetty.util.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashTransposeHopDag;

public class ConcurrentBaoLi {

    protected static final Log LOG = LogFactory.getLog(ConcurrentBaoLi.class.getName());

    private static int threadNum = 12;

    //private static ArrayList<Hop> dags;
    private static HopRewriteRule rewriteCommonSubexpressionElimination = new RewriteCommonSubexpressionElimination();
    public static ConstantUtil constantUtil = new ConstantUtil(null);


    private final static ConcurrentHashSet<Pair<Long, Long>> hashKeysSet = new ConcurrentHashSet<>();
    private static ArrayList<MyRule> rules;

    static {
        rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
//        rules.add(new TransposeMatrixMatrixMultMergeRule());
//        rules.add(new TransposeMatrixMatrixMultSplitRule());
    }

    private static final ConcurrentLinkedQueue<Hop> unappliedDags = new ConcurrentLinkedQueue<>();
    private static RewriteCoordinate rewriteCoordinateEstimator;

    public static MySolution generateAllTrees(Hop root, RewriteCoordinate rewriteCoordinateEstimator) {
        ConcurrentBaoLi.rewriteCoordinateEstimator = rewriteCoordinateEstimator;
//        minHop = null;
        minSolution = null;
        minCost = Double.MAX_VALUE;
//        System.out.println("<--");
        //  dags = new ArrayList<>();
        hashKeysSet.clear();
        //  dags.add(root); // push
        unappliedDags.add(root);

        hashKeysSet.add(hashHopDag(root));
        // path = new ArrayList<>();
        LOG.info("root=");
        root.resetVisitStatusForced(new HashSet<>());
        LOG.info(Explain.explain(root));

//        for (int i = 0; i < dags.size(); i++) {
//            firstDag = dags.get(i);
//            // System.out.println(Explain.explain(firstDag));
//            generate_iter(firstDag, 0);
//            //  break;
//        }
        CountDownLatch latch = new CountDownLatch(threadNum);
        ExecutorService fixedThreadPool = Executors.newCachedThreadPool();

        for (int i = 0; i < threadNum; i++) {
            fixedThreadPool.execute(new AAA(latch));
        }

        try {
            latch.await();
        } catch (InterruptedException E) {
            // handle
        }

        fixedThreadPool.shutdownNow();
        try {
            boolean terminated = fixedThreadPool.awaitTermination(3, TimeUnit.SECONDS);
            LOG.info("terminated:" + terminated);
        } catch (Exception e) {
            e.printStackTrace();
        }


        hashKeysSet.clear();
        unappliedDags.clear();


        return minSolution;


    }

    static Double minCost = Double.MAX_VALUE;
    //    static Hop minHop = null;
    static MySolution minSolution = null;

    private static class AAA implements Runnable {

        private ArrayList<Integer> path = new ArrayList<>();
        private Hop firstDag;

        CountDownLatch latch;

        public AAA(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    int treesNum = hashKeysSet.size();
                    int queueSize = unappliedDags.size();
                    if (queueSize % 1000 == 0) {
                        LOG.info("all trees number: " + treesNum + ", queue size: " + queueSize);
                        System.out.println(java.time.LocalTime.now() + " trees number: " + treesNum + ", queue size: " + queueSize);
                    }
                    Hop root = null;
                    for (int i = 0; i < 3; i++) {
                        root = unappliedDags.poll();
                        if (root != null) {
                            break;
                        } else {
                            Thread.sleep(1000);
                        }
                    }
                    if (root == null) {
                        break;
                    } else {
                        try {
                            firstDag = root;
                            // System.out.println("rootid=" + root.getHopID());
                            generate_iter(root, 0);
                            findCseAndLse(firstDag);
                            //   updateAns(firstDag);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                LOG.info("stop");
                latch.countDown();
            }
        }


        static void findCseAndLse(Hop hop) {
            hop = rewriteCommonSubexpressionElimination.rewriteHopDAG(hop, new ProgramRewriteStatus());
            MySolution solution = constantUtil.liftLoopConstant(hop);
        }


        static void updateAns(Hop hop) {
            //  try {
            hop = rewriteCommonSubexpressionElimination.rewriteHopDAG(hop, new ProgramRewriteStatus());
            MySolution solution = new MySolution(hop);
            double cost = rewriteCoordinateEstimator.estimate(solution, false); // estimate
            synchronized (minCost) {
                if (cost < minCost) {
                    minCost = cost;
//                        minHop = hop;
                    minSolution = solution;
                }
            }
//            }catch (Exception e) {
//                e.printStackTrace();
//            }
        }

        private void generate_iter(Hop current, int depth) {
            //   System.out.println("F2: dep: "+depth+" , size "+path.size());
            for (int i = 0; i < current.getInput().size(); i++) {
                if (path.size() <= depth) path.add(i);
                else path.set(depth, i);
                generate_iter(current.getInput().get(i), depth + 1);
            }
            for (MyRule rule : rules) {
                //  System.out.println("call F3: dep: "+depth+" , id: "+current.getHopID()+" rule: "+rule);
                if (rule.applicable(null, current, 0)) {
                    copyChangePush(depth, rule);
                }
            }
        }

        private void copyChangePush(int depth, MyRule rule) {
//             System.out.println("F3: dep: "+depth);
            Hop shadowDag = DeepCopyHopsDag.deepCopyHopsDag(firstDag);
            Hop parentNode = null;
            Hop currentNode = shadowDag;
            for (int d = 0; d < depth; d++) {
                int i = path.get(d);
//                System.out.print(" " + i);
                parentNode = currentNode;
                currentNode = currentNode.getInput().get(i);
            }
//            System.out.println("");
            currentNode = rule.apply(parentNode, currentNode, 0);
            if (parentNode == null) shadowDag = currentNode;
            Pair<Long, Long> hash = hashHopDag(shadowDag);
//            shadowDag.resetVisitStatus();
//            System.out.println("the new tree is:");
//            System.out.println(Explain.explain(shadowDag));
//            System.out.println("new tree: hash=" + hash);
//            shadowDag.resetVisitStatus();
//            System.out.println(Explain.explain(shadowDag));
            synchronized (hashKeysSet) {
                if (!hashKeysSet.contains(hash)) {
                    //  System.out.println("push " + shadowDag.getHopID() + " " + unappliedDags.size() + " " + hashKeysSet.size());
                    hashKeysSet.add(hash);
                    unappliedDags.add(shadowDag);
                    //   System.out.println(unappliedDags.size());
//                    if (hashKeysSet.size()%1000==0) {
//                        System.out.println(hashKeysSet.size());
//                    }
                }
            }

//            else {
//                System.out.println("don't push");
//            }

        }

    }


}
