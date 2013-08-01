package cascading.operation.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * Class EqualsValue verifies that the first n values in the argument values {@link cascading.tuple.Tuple} equals the n
 * values specified by the constructor. If a non-equal value is encountered, or if the tuple does not have enough
 * values, the current Tuple will be filtered out.
 */
public class EqualsValue extends BaseOperation<Void> implements Filter<Void> {

  private static final long serialVersionUID = 1L;

  private Object[] values;

  public EqualsValue(Object value, Object... values) {
    super(1);
    this.values = new Object[values.length + 1];
    this.values[0] = value;
    System.arraycopy(values, 0, this.values, 1, values.length);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
    TupleEntry arguments = filterCall.getArguments();
    if (arguments.size() < values.length) {
      return true;
    }
    for (int i = 0; i < values.length; i++) {
      Object arg = arguments.getObject(i);
      if (values[i] == null || arg == null) {
        if (values[i] != null || arg != null) {
          return true;
        }
      }
      if (!values[i].equals(arg)) {
        return true;
      }
    }
    return false;
  }
}
