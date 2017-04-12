package cascading.datatypeissue;

import org.junit.Test;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.platform.PlatformRunner.Platform;
import cascading.platform.hadoop2.Hadoop2MR1Platform;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import static data.InputData.testDataTypesInput;

import java.io.IOException;
import java.util.List;

@Platform({Hadoop2MR1Platform.class})
public class TestDatatypeWhileSink extends PlatformTestCase {

	/**
	 *  
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * It should throw NumberFormatException for field 'name' while sinking as Integer.
	 * The sink tap writes the 'name' field as in the input file. But while reading the sink tap the value in name field is null.
	 */
	@Test
	public void itshouldThrowErrorForIncorrectDatatype(){
		
		Class types1[] = { Integer.class, String.class, Integer.class };
        Fields f1 = new Fields("id", "name", "amt");
        Tap source = getPlatform().getDelimitedFile(f1, ",", types1, testDataTypesInput, SinkMode.KEEP);

        Class classarray[] = { Integer.class, Integer.class, Integer.class };
        Fields f2 = new Fields("id", "name", "amt");
        Tap sink =getPlatform().getDelimitedFile(f2, false, ",", null, classarray, getOutputPath("output/DatatypeIssue"), SinkMode.REPLACE); 

        Pipe pipe = new Pipe("ReadWrite");
        
        Flow flow = getPlatform().getFlowConnector().connect(source, sink, pipe);
        
        flow.complete();
        List<Tuple> tupleList = null;
        try {
			tupleList = asList(flow, sink);
		} catch (IOException e) {
			e.printStackTrace();
		}
        assertTrue(tupleList.contains(new Tuple(1 ,null,3546)));
        
	}

}
