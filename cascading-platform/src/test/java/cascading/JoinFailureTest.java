package cascading;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.LeftJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public class JoinFailureTest extends PlatformTestCase {
    public static final SingleValueOptimizationComparator
        SINGLE_VALUE_OPTIMIZATION_COMPARATOR = 
            new SingleValueOptimizationComparator();

    private static final String WORKING_PATH_NAME = "build/test/JoinFailureTest/";

    public JoinFailureTest() {
    }

    @Test
    public void testComparator() throws Throwable {
        assertEquals(0, SINGLE_VALUE_OPTIMIZATION_COMPARATOR.compare("all", "us&all"));
        assertEquals(0, SINGLE_VALUE_OPTIMIZATION_COMPARATOR.compare("us&all", "all"));

        assertEquals(0, SINGLE_VALUE_OPTIMIZATION_COMPARATOR.compare("us", "us&all"));
        assertEquals(0, SINGLE_VALUE_OPTIMIZATION_COMPARATOR.compare("us&all", "us"));
    }
    
    @Test
    @SuppressWarnings("rawtypes")
    public void testJoinFailure() throws Throwable {
        fail("make sure this test is being run!");
        File workingDir = new File(WORKING_PATH_NAME + "testJoinFailure");
        File lhsDir = new File(workingDir, "lhs");
        File rhsDir = new File(workingDir, "rhs");
        File outputDir = new File(workingDir, "output");
        
        // LHS has one Tuple with us and one with all
        Fields lhsFields = new Fields("lhs-country", "lhs-index", "lhs-expected-hit-count");
        List<Tuple> lhsTuples = new ArrayList<Tuple>();
        lhsTuples.add(new Tuple("us", "lhs-1", "1"));
        lhsTuples.add(new Tuple("all", "lhs-2", "1"));
        writeTuples(lhsDir, lhsFields, lhsTuples);
        
        // RHS us&all Tuple should match both
        Fields rhsFields = new Fields("rhs-country", "rhs-index");
        List<Tuple> rhsTuples = new ArrayList<Tuple>();
        rhsTuples.add(new Tuple("us&all", "rhs-1"));
        writeTuples(rhsDir, rhsFields, rhsTuples);
        
        // Create and run a flow that CoGroups two pipes with a LeftJoin,
        // using the Comparator to group the keys on both sides.
        Map<String, Tap> sourceTaps = new HashMap<String, Tap>();
        Tap lhsSource = 
            getPlatform().getDelimitedFile( lhsFields,
                                            "\t", 
                                            lhsDir.toString());
        Pipe lhsPipe = new Pipe("lhs Tuples");
        sourceTaps.put(lhsPipe.getName(), lhsSource);
        
        Tap rhsSource = 
            getPlatform().getDelimitedFile( rhsFields,
                                            "\t", 
                                            rhsDir.toString());
        Pipe rhsPipe = new Pipe("rhs Tuples");
        sourceTaps.put(rhsPipe.getName(), rhsSource);
        
        Fields lhsKeyFields = new Fields("lhs-country");
        lhsKeyFields.setComparator( "lhs-country", 
                                    SINGLE_VALUE_OPTIMIZATION_COMPARATOR);
        Fields rhsKeyFields = new Fields("rhs-country");
        rhsKeyFields.setComparator( "rhs-country", 
                                    SINGLE_VALUE_OPTIMIZATION_COMPARATOR);
        Pipe outputPipe = 
            new CoGroup("output Tuples", 
                        lhsPipe, lhsKeyFields,
                        rhsPipe, rhsKeyFields,
                        new LeftJoin());
        outputPipe = new Each(  outputPipe, 
                                new Debug(outputPipe.getName(), true));
        
        Fields outputFields = lhsFields.append(rhsFields);
        

        Tap outputSink = 
            getPlatform().getDelimitedFile( outputFields,
                                            "\t", 
                                            outputDir.toString(), 
                                            SinkMode.REPLACE);
                        
        FlowConnector flowConnector = getPlatform().getFlowConnector();
        Flow flow = flowConnector.connect(  "Left-joining lhs with rhs",
                                            sourceTaps, 
                                            outputSink,
                                            outputPipe);
        flow.start();
        flow.complete();
        
        // Verify that every LHS Tuple makes it through at least once, and that
        // we get the expected number of hits (or just a single miss).
        Map<Integer, Integer> lhsExpectedHitCountMap = 
            new HashMap<Integer, Integer>();
        for (int lhsTupleIndex = 0; lhsTupleIndex < lhsTuples.size(); lhsTupleIndex++) {
            TupleEntry lhsTupleEntry = 
                new TupleEntry(lhsFields, lhsTuples.get(lhsTupleIndex));
            String expectedHitCountString = 
                lhsTupleEntry.getString("lhs-expected-hit-count");
            lhsExpectedHitCountMap.put( lhsTupleIndex, 
                                        Integer.parseInt(expectedHitCountString));
        }
        List<Tuple> outputTuples = readTuples(outputDir, outputFields);
        
        // Note: Sometimes it fails here, depending on various unrelated
        // changes to source code (e.g., which package unit test lives in):
        assertTrue( "at least one LHS input Tuple got stripped out (LeftJoin)",
                    outputTuples.size() >= lhsTuples.size());
        
        for (Tuple outputTuple : outputTuples) {
            TupleEntry outputTupleEntry = 
                new TupleEntry(outputFields, outputTuple);
            int lhsTupleIndex = 0;
            for (Tuple lhsTuple : lhsTuples) {
                TupleEntry lhsTupleEntry = 
                    new TupleEntry(lhsFields, lhsTuple);
                boolean isLhsMatch = true;
                for (Comparable lhsField : lhsFields) {
                    if (!(lhsTupleEntry.getString(lhsField)
                            .equals(outputTupleEntry.getString(lhsField)))) {
                        isLhsMatch = false;
                        break;
                    }
                }
                if (isLhsMatch) {
                    break;
                }
                lhsTupleIndex++;
            }
            assertTrue( (   "Output Tuple doesn't match any LHS Tuple: "
                        +   outputTupleEntry), 
                        lhsTupleIndex < lhsTuples.size());
            int hitCount = lhsExpectedHitCountMap.get(lhsTupleIndex);
            if (hitCount > 0) {
                if (hitCount == 1) {
                    hitCount = 0;
                }
                lhsExpectedHitCountMap.put(lhsTupleIndex, hitCount - 1);
                for (Comparable rhsField : rhsFields) {

                    // Note: Sometimes it fails here, depending on various
                    // unrelated changes to source code (e.g., which package
                    // unit test lives in):
                    assertNotNull(  (   "RHS fields of output Tuple should be non-null (since it should have been a hit): "
                                    +   outputTupleEntry), 
                                    outputTupleEntry.getString(rhsField));
                }
            } else {
                TupleEntry lhsTupleEntry = 
                    new TupleEntry(lhsFields, lhsTuples.get(lhsTupleIndex));
                assertEquals(   (   "Too many hits for LHS Tuple: "
                                +   lhsTupleEntry),
                                0, hitCount);
                lhsExpectedHitCountMap.put(lhsTupleIndex, hitCount - 1);
                for (Comparable rhsField : rhsFields) {
                    assertNull( (   "RHS fields of output Tuple should all be null (since it should have been a miss): "
                                +   outputTupleEntry), 
                                outputTupleEntry.getString(rhsField));
                }
            }
        }
        for (int lhsTupleIndex = 0; lhsTupleIndex < lhsTuples.size(); lhsTupleIndex++) {
            TupleEntry lhsTupleEntry = 
                new TupleEntry(lhsFields, lhsTuples.get(lhsTupleIndex));
            int expectedHitCount = 
                Integer.parseInt(lhsTupleEntry.getString("lhs-expected-hit-count"));
            String message = 
                (   (expectedHitCount == 0) ?
                    String.format(  "Expected exactly %d hits for LHS Tuple: %s",
                                    expectedHitCount,
                                    lhsTupleEntry)
                :   String.format(  "Expected exactly 1 miss output Tuple for LHS Tuple: %s",
                                    lhsTupleEntry));
            assertEquals(   message, 
                            -1, 
                            (int)lhsExpectedHitCountMap.get(lhsTupleIndex));
        }
    }
    
    public static class SingleValueOptimizationComparator
        implements Comparator<String>, Hasher<String>, Serializable {
    
        private SingleValueOptimizationComparator() {
            super();
        }
    
        @Override
        public int compare(String thisValue, String thatValue) {
            
            if (thisValue == null) {
                if (thatValue == null) {
                    return 0;
                }
                return -1;
            } else if (thatValue == null) {
                return 1;
            }
            
            if (thisValue.equals(thatValue)) {
                return 0;
            }
            
            // us and all each equal us&all
            if (thisValue.equals("us&all")) {
                if  (   thatValue.equals("us")
                    ||  thatValue.equals("all")) {
                    return 0;
                }
            }
            if (thatValue.equals("us&all")) {
                if  (   thisValue.equals("us")
                    ||  thisValue.equals("all")) {
                    return 0;
                }
            }
            
            return thisValue.compareTo(thatValue);
        }
    
        @Override
        public int hashCode(String value) {
            
            // The transitive property of hash code equality forces virtually
            // all possible country values to share the same hash code
            // (whenever the Tuples in the Pipe have already been optimized to
            // combine the otherwise identical single "specific" Tuple with its
            // "all" aggregate).  For example:
            // 1) us must equal us&all.
            // 2) all must equal us&all.
            // 3) Therefore, us must equal all.
            // 4) Similarly, us must equal gb and every other specific country.
            // Therefore, we just hash every value to 0.  
            // Note that this means such fields can't participate in partitioning
            // the Tuples for the reduce phase.
            // Happily, the grouping key typically includes non-optimized fields
            // (e.g., advertiser) so all Tuples aren't sent to the same reducer.
            return 0;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void writeTuples(File targetDir,
                            Fields outputFields,
                            List<Tuple> tuples)
        throws Exception {
        
        Tap sinkTap = 
            getPlatform().getDelimitedFile( outputFields,
                                            "\t", 
                                            targetDir.toString(), 
                                            SinkMode.REPLACE);
        
        TupleEntryCollector tupleWriter = null;
        try {
            tupleWriter = 
                sinkTap.openForWrite(getPlatform().getFlowProcess());
            for (Tuple tuple : tuples) {
                tupleWriter.add(tuple);
            }
        } finally {
            if (tupleWriter != null) {
                tupleWriter.close();
            }
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<Tuple> readTuples(File sourceDir, Fields inputFields) 
        throws Exception {
        
        Tap sourceTap = 
            getPlatform().getDelimitedFile( inputFields,
                                            "\t", 
                                            sourceDir.toString());
        Iterator<TupleEntry> iter = 
            sourceTap.openForRead(getPlatform().getFlowProcess());
        List<Tuple> result = new ArrayList<Tuple>();
        while (iter.hasNext()) {
            result.add(iter.next().getTuple());
        }
        return result;
    }
}
