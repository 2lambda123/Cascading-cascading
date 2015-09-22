package cascading.tap.partition;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Objects;

import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.TupleEntry;

public class PartitionTapFilter implements Serializable {

  private final Filter filter;
  private final Fields argumentSelector;
  private transient ConcreteCall<?> filterCall;

  PartitionTapFilter(Filter filter, Fields argumentSelector) {
    this.filter = filter;
    this.argumentSelector = argumentSelector;
  }

  void prepare(FlowProcess flowProcess) {
    filter.prepare(flowProcess, getFilterCall());
  }

  boolean isRemove(FlowProcess flowProcess, TupleEntry partitionEntry) {
    return filter.isRemove(flowProcess, getFilterCall(partitionEntry.selectEntry(argumentSelector)));
  }

  void cleanup(FlowProcess flowProcess) {
    filter.cleanup(flowProcess, getFilterCall());
  }
  
  private ConcreteCall<?> getFilterCall(TupleEntry arguments) {
    getFilterCall();
    filterCall.setArguments(arguments);
    return filterCall;
  }
  
  private ConcreteCall<?> getFilterCall() {
    if (filterCall == null) {
      filterCall = new ConcreteCall(argumentSelector);
    }
    return filterCall;
  }
  
  public static boolean partitionContainsArguments(Fields partition, Fields argument) {
    for (int i = 0; i < argument.size(); i++) {
      Comparable comparable = argument.get(i);
      int pos;
      try {
        pos = partition.getPos(comparable);
      } catch (FieldsResolverException e) {
        return false;
      }
      Type argumentType = argument.getType(i);
      Type partitionType = partition.getType(pos);
      if (!Objects.equals(argumentType, partitionType)) {
        return false;
      }
    }
    return true;
  }

}
