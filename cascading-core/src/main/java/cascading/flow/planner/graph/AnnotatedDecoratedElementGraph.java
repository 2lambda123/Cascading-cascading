/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.flow.planner.graph;

import cascading.flow.FlowElement;
import cascading.util.EnumMultiMap;

/**
 *
 */
public class AnnotatedDecoratedElementGraph extends DecoratedElementGraph implements AnnotatedGraph
  {
  private final EnumMultiMap<FlowElement> annotations;

  public AnnotatedDecoratedElementGraph( ElementGraph decorated, EnumMultiMap<FlowElement> annotations )
    {
    super( decorated );

    if( annotations == null )
      throw new IllegalArgumentException( "annotations are required" );

    this.annotations = annotations;
    }

  @Override
  public boolean hasAnnotations()
    {
    return annotations != null && !annotations.isEmpty();
    }

  @Override
  public EnumMultiMap<FlowElement> getAnnotations()
    {
    return annotations;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( annotations != null ? annotations.hashCode() : 0 );
    return result;
    }
  }
