/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tuple.util;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import cascading.flow.stream.MemorySpliceGate;
import cascading.tuple.Fields;
import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TupleHasher implements Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( MemorySpliceGate.class );

  private static Hasher DEFAULT = new ObjectHasher();
  private Hasher[] hashers;

  public TupleHasher()
    {
    }

  public TupleHasher( Comparator defaultComparator, Comparator[] comparators )
    {
    initialize( defaultComparator, comparators );
    }

  public static Comparator[] merge( Fields[] keyFields )
    {
    Comparator[] comparators = new Comparator[ keyFields[ 0 ].size() ];

    for( Fields keyField : keyFields )
      {
      if( keyField == null )
        continue;

      for( int i = 0; i < keyField.getComparators().length; i++ )
        {
        Comparator comparator = keyField.getComparators()[ i ];

        if( !( comparator instanceof Hasher ) )
          continue;

        if( comparators[ i ] != null && !comparators[ i ].equals( comparator ) )
          LOG.warn( "two unequal Hasher instances for the same key field position found: {}, and: {}", comparators[ i ], comparator );

        comparators[ i ] = comparator;
        }
      }

    return comparators;
    }

  public static boolean isNull( Comparator[] comparators )
    {
    int count = 0;

    for( Comparator comparator : comparators )
      {
      if( comparator == null )
        count++;
      }

    if( count == comparators.length )
      return true;

    return false;
    }

  protected void initialize( Comparator defaultComparator, Comparator[] comparators )
    {
    Hasher defaultHasher = DEFAULT;

    if( defaultComparator instanceof Hasher )
      defaultHasher = (Hasher) defaultComparator;

    hashers = new Hasher[ comparators.length ];

    for( int i = 0; i < comparators.length; i++ )
      {
      Comparator comparator = comparators[ i ];

      if( comparator instanceof Hasher )
        hashers[ i ] = (Hasher) comparator;
      else
        hashers[ i ] = defaultHasher;
      }
    }

  public final int hashCode( Tuple tuple )
    {
    List<Object> elements = Tuple.elements( tuple );
    int size = elements.size();

    if( size > 0 )
      {
      Object element0 = elements.get(0);
      int hash = element0 != null ? hashers[ 0 ].hashCode( element0 ) : 0;

      for( int i = 1; i < size; i++ )
        {
        Object element = elements.get( i );

        hash = 31 * hash + ( element != null ? hashers[ i % hashers.length ].hashCode( element ) : 0 );
        }

        return hash;
      }
    else
      return 0;
    }

  private static class ObjectHasher implements Hasher<Object>
    {
    @Override
    public int hashCode( Object value )
      {
      return value.hashCode();
      }
    }

  static class WrappedTuple extends Tuple
    {
    private final TupleHasher tupleHasher;

    public WrappedTuple( TupleHasher tupleHasher, Tuple input )
      {
      super( Tuple.elements( input ) );
      this.tupleHasher = tupleHasher;
      }

    @Override
    public int hashCode()
      {
      return tupleHasher.hashCode( this );
      }
    }

  /**
   * Wraps the given Tuple in a subtype, that uses the provided Hasher for hashCode calculations. If the given Hahser
   * is <code>null</code> the input Tuple will be returned.
   *
   * @param tupleHasher A TupleHasher instance.
   * @param input       A Tuple instance.
   * @return A tuple using the provided TupleHasher for hashCode calculations if the TupleHasher is not null.
   */
  public static Tuple wrapTuple( TupleHasher tupleHasher, Tuple input )
    {
    if( tupleHasher == null )
      return input;

    return new WrappedTuple( tupleHasher, input );
    }
  }
