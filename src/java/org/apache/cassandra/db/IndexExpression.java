/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.LocalToken;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexExpression
{
    public final ByteBuffer column;
    public final Operator operator;
    public final ByteBuffer value;

    public IndexExpression(ByteBuffer column, Operator operator, ByteBuffer value)
    {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    public enum Operator
    {
        EQ, GTE, GT, LTE, LT, CONTAINS, CONTAINS_KEY;

        public static Operator findByOrdinal(int ordinal)
        {
            switch (ordinal) {
                case 0:
                    return EQ;
                case 1:
                    return GTE;
                case 2:
                    return GT;
                case 3:
                    return LTE;
                case 4:
                    return LT;
                case 5:
                    return CONTAINS;
                case 6:
                    return CONTAINS_KEY;
                default:
                    throw new AssertionError();
            }
        }

        public boolean allowsIndexQuery()
        {
        	return true;
        }
    }

    public boolean isSingleValueQuery() 
    {
        switch (operator)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                return true;
            default:
                return false;
        }
    }
    
    public boolean isMinValueQuery() 
    {
        switch (operator)
        {
            case GT:
            case GTE:
                return true;
            default:
                return false;
        }
    }
    
    public boolean isMaxValueQuery() 
    {
        switch (operator)
        {
            case LT:
            case LTE:
                return true;
            default:
                return false;
        }
    }
    
    public boolean isInRange(ByteBuffer value)
    {
        int comparison = value.compareTo(this.value);
        
        switch (operator)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                return false;
        }
    }
    
    @Override
    public String toString()
    {
        return String.format("%s %s %s", ByteBufferUtil.bytesToHex(column), operator, ByteBufferUtil.bytesToHex(value));
    }

    /*
    public RowPosition start(AbstractType<?> comparator)
    {
        switch (operator)
        {
            case GTE:
            case GT:
                return new BufferDecoratedKey(new LocalToken(comparator, value), value); 
            case LTE:
            case LT:
                return RowPosition.Kind.MIN_BOUND;
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
            default:    
                return new BufferDecoratedKey(new LocalToken(comparator, value), value);
        }
    }
    
    public RowPosition end(AbstractType<?> comparator)
    {
        switch (operator)
        {
            case GTE:
            case GT:
                return value; //TODO absolute max
            case LTE:
            case LT:
                return new BufferDecoratedKey(new LocalToken(comparator, value), value);
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
            default:    
                return new BufferDecoratedKey(new LocalToken(comparator, value), value);
        }
    }*/
    
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof IndexExpression))
            return false;

        IndexExpression ie = (IndexExpression) o;

        return Objects.equal(this.column, ie.column)
            && Objects.equal(this.operator, ie.operator)
            && Objects.equal(this.value, ie.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, operator, value);
    }
}
