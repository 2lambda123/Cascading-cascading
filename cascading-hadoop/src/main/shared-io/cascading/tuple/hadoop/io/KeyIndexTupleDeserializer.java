/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop.io;

import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.KeyIndexTuple;

public class KeyIndexTupleDeserializer extends IndexTupleDeserializer<KeyIndexTuple>
  {
  public KeyIndexTupleDeserializer( TupleSerialization.SerializationElementReader elementReader )
    {
    super( elementReader );

    typeMap = elementReader.getTupleSerialization().getKeyTypeMap();

    if( elementReader.getTupleSerialization().areTypesRequired() )
      {
      if( typeMap == null || typeMap.isEmpty() )
        throw new IllegalStateException( "types are required to perform serialization, fields: " + elementReader.getTupleSerialization().getKeyFieldsMap() );
      }
    }

  @Override
  protected KeyIndexTuple createTuple()
    {
    return new KeyIndexTuple();
    }

  @Override
  protected Class[] getTypesFor( int ordinal )
    {
    return typeMap.get( ordinal );
    }
  }