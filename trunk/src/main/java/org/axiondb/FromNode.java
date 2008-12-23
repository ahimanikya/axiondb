/* 
 * =======================================================================
 * Copyright (c) 2003-2004 Axion Development Team.  All rights reserved.
 *  
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met:
 * 
 * 1. Redistributions of source code must retain the above 
 *    copyright notice, this list of conditions and the following 
 *    disclaimer. 
 *   
 * 2. Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the 
 *    distribution. 
 *   
 * 3. The names "Tigris", "Axion", nor the names of its contributors may 
 *    not be used to endorse or promote products derived from this 
 *    software without specific prior written permission. 
 *  
 * 4. Products derived from this software may not be called "Axion", nor 
 *    may "Tigris" or "Axion" appear in their names without specific prior
 *    written permission.
 *   
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * =======================================================================
 */
package org.axiondb;

/**
 * A binary tree of tables (or "table like" objects) being selected from. Each element in
 * the tree is either a FromNode or a TableIdentifier or a sub-query.
 * 
 * @version  
 * @author Amrish Lal
 */
public class FromNode {

    /** Inner join. */
    public static final int TYPE_INNER = 1;

    /** Left outer join */
    public static final int TYPE_LEFT = 2;

    /** Right outer join */
    public static final int TYPE_RIGHT = 3;

    /** No Join */
    public static final int TYPE_SINGLE = 0;

    public FromNode() {
    }

    /**
     * get the join condition
     * 
     * @return join condition.
     */
    public Selectable getCondition() {
        return _condition;
    }

    /**
     * Get the Left input
     * 
     * @return Object of type {@link FromNode}or {@link TableIdentifier}
     */
    public Object getLeft() {
        return _left;
    }

    /**
     * get the right input
     * 
     * @return Object of type {@link FromNode}or {@link TableIdentifier}
     */
    public Object getRight() {
        return _right;
    }

    /**
     * Number of tables in this FromNode and its children.
     * 
     * @return table count.
     */
    public int getTableCount() {
        return getTableCount(getLeft()) + getTableCount(getRight());
    }

    /**
     * get the type of the join
     * 
     * @return integer indicating type (UNDEFINED, LEFT OUTER, RIGHT OUTER, INNER)
     */
    public int getType() {
        return _type;
    }

    public boolean hasCondition() {
        return null != getCondition();
    }

    public boolean hasLeft() {
        return null != getLeft();
    }

    public boolean hasRight() {
        return null != getRight();
    }

    public boolean isInnerJoin() {
        return getType() == FromNode.TYPE_INNER;
    }

    public boolean isLeftJoin() {
        return getType() == FromNode.TYPE_LEFT;
    }

    public boolean isRightJoin() {
        return getType() == FromNode.TYPE_RIGHT;
    }

    /**
     * Set the join condition
     * 
     * @param type condition Join condition.
     */
    public void setCondition(Selectable condition) {
        _condition = condition;
    }

    public void setLeft(FromNode join) {
        _left = join;
    }

    public void setLeft(Object table) {
        _left = table;
    }

    public void setRight(FromNode join) {
        _right = join;
    }

    public void setRight(Object table) {
        _right = table;
    }

    /**
     * Set the type of join.
     * 
     * @param type integer value representing join type (INNER, LEFT OUTER, RIGHT OUTER)
     */
    public void setType(int type) {
        _type = type;
    }

    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Array of tables in this FromNode or its children. Array is devleoped by preorder
     * traversal of the FromNode tree.
     * 
     * @return Array of {@link TableIdentifier}
     */
    public TableIdentifier[] toTableArray() {
        TableIdentifier[] tables = new TableIdentifier[getTableCount()];
        toTableArray(tables, 0);
        return (tables);
    }

    private String toString(String prefix) {
        String result = "\n";
        result += prefix + "Type : " + typeToString(getType()) + "\n";
        if (_left instanceof TableIdentifier) {
            TableIdentifier table = (TableIdentifier) _left;
            result += prefix + "Left : " + " TableIdentifier " + table.toString() + "\n";
        }
        if (_left instanceof FromNode) {
            FromNode node = (FromNode) _left;
            result += prefix + "Left : " + " FromNode " + node.toString(prefix + "\t") + "\n";
        }

        if (_right instanceof TableIdentifier) {
            TableIdentifier table = (TableIdentifier) _right;
            result += prefix + "Right: " + " TableIdentifier " + table.toString() + "\n";
        }

        if (_right instanceof FromNode) {
            FromNode node = (FromNode) _right;
            result += prefix + "Right: " + " FromNode " + node.toString(prefix + "\t") + "\n";
        }
        return (result);
    }

    private int toTableArray(TableIdentifier[] tables, int pos) {
        pos = toTableArray(tables, pos, getLeft());
        pos = toTableArray(tables, pos, getRight());
        return pos;
    }

    public static String typeToString(int type) {
        switch (type) {
            case TYPE_SINGLE:
                return "single";
            case TYPE_INNER:
                return "inner";
            case TYPE_LEFT:
                return "left-outer";
            case TYPE_RIGHT:
                return "right-outer";
            default:
                return "unknown?";
        }
    }

    private static int getTableCount(Object child) {
        if (null == child) {
            return 0;
        } else if (child instanceof TableIdentifier) {
            return 1;
        } else {
            return ((FromNode) child).getTableCount();
        }
    }

    private static int toTableArray(TableIdentifier[] tables, int pos, Object child) {
        if (null == child) {
            return pos;
        } else if (child instanceof TableIdentifier) {
            tables[pos] = ((TableIdentifier) child);
            return pos + 1;
        } else {
            return ((FromNode) child).toTableArray(tables, pos);
        }
    }

    /** Join condition */
    private Selectable _condition = null;

    /** Left input table identifier or FromNode. */
    private Object _left = null;

    /** Right input table identifier or FromNode. */
    private Object _right = null;

    /** Join type */
    private int _type = TYPE_SINGLE;
}
