package cascading.operation.filter;

import cascading.tuple.Tuple;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static cascading.CascadingTestCase.invokeFilter;

public class EqualsValueTest {
  @Test
  public void testEqualsValue() {
    EqualsValue e1 = new EqualsValue("aaa");
    Tuple[] args1 = new Tuple[] {
        new Tuple("aaa"),
        new Tuple("bbb"),
        new Tuple(1),
        new Tuple(null)
    };
    boolean[] actual1 = invokeFilter(e1, args1);
    assertEquals(false, actual1[0]);
    assertEquals(true, actual1[1]);
    assertEquals(true, actual1[2]);
    assertEquals(true, actual1[3]);
    
    EqualsValue e2 = new EqualsValue(1, "x");
    Tuple[] args2 = new Tuple[] {
        new Tuple(1, "x"),
        new Tuple(2, "x"),
        new Tuple(1, "y"),
        new Tuple(1)
    };
    boolean[] actual2 = invokeFilter(e2, args2);
    assertEquals(false, actual2[0]);
    assertEquals(true, actual2[1]);
    assertEquals(true, actual2[2]);
    assertEquals(true, actual2[3]);
  }
}
