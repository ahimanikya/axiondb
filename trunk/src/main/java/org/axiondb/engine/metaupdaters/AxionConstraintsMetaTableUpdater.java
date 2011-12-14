/*
 * $Id: AxionConstraintsMetaTableUpdater.java,v 1.2 2007/12/12 13:35:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.metaupdaters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;
import org.axiondb.AxionException;
import org.axiondb.Constraint;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.Selectable;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.constraints.PrimaryKeyConstraint;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.BaseDatabaseModificationListener;
import org.axiondb.event.ColumnEvent;
import org.axiondb.event.ConstraintEvent;
import org.axiondb.event.DatabaseModifiedEvent;
import org.axiondb.event.RowEvent;
import org.axiondb.event.TableModificationListener;

/**
 *
 * @author Manish Bharani
 */
public class AxionConstraintsMetaTableUpdater extends BaseDatabaseModificationListener implements TableModificationListener {
    
    private static Logger _log = Logger.getLogger(AxionConstraintsMetaTableUpdater.class.getName());
    private Database _db = null;
    private ArrayList addedKeys = new ArrayList<String>();
    private static int PK_SEQ = 0;
    private static int FK_SEQ = 0;
    
    
    public AxionConstraintsMetaTableUpdater() {
    }
    
    /**
     * Construstor for AxionConstraintsMetaTableUpdater
     * @param db - Database Object
     */
    public AxionConstraintsMetaTableUpdater(Database db) {
        _db = db;
    }
    
    private ArrayList createPkFkConstraintRows(Constraint pkFkConstraint, String tablename){
        ArrayList rowlist = new ArrayList<Row>();
        
        if (pkFkConstraint instanceof PrimaryKeyConstraint){
            for (int i=0; i < ((PrimaryKeyConstraint)pkFkConstraint).getSelectableCount(); i++){
                // Insert Data for the Primary keys
                rowlist.add(createPKConstraintRow((PrimaryKeyConstraint)pkFkConstraint, tablename, ((PrimaryKeyConstraint)pkFkConstraint).getSelectable(i)));
            }
        } else if (pkFkConstraint instanceof ForeignKeyConstraint){
            // Insert Data for the Foreign Key
            rowlist.add(createFKConstraintRow((ForeignKeyConstraint)pkFkConstraint, tablename));
        }
        return rowlist;
    }
    
    
    private Row createPKConstraintRow(PrimaryKeyConstraint constraint, String table, Selectable s){
        SimpleRow constraintrow = new SimpleRow(14);
        constraintrow.set(0,++PK_SEQ);                    // PK_Key_Seq
        constraintrow.set(1,"");                          // PK_TABLE_CAT
        constraintrow.set(2,"");                          // PK_TABLE_SCHEM
        constraintrow.set(3,table);                       // PK_TABLE_NAME
        constraintrow.set(4,constraint.getName());        // PK_NAME
        constraintrow.set(5,s.getName());                 // PK_COLUMN_NAME
        return constraintrow;
    }
    
    private Row createFKConstraintRow( ForeignKeyConstraint constraint, String table){
        SimpleRow constraintrow = new SimpleRow(14);
        constraintrow.set(0,++FK_SEQ);                                      // PK_Key_Seq
        constraintrow.set(1, "");                                           // PK_TABLE_CAT
        constraintrow.set(2, "");                                           // PK_TABLE_SCHEM
        constraintrow.set(3,constraint.getParentTableName());               // PK_TABLE_NAME
        //Assuming Only One parent column exists
        if (constraint.getParentTableColumns() != null){
            constraintrow.set(5,((Selectable)constraint.getParentTableColumns().get(0)).getName()); // PK_COLUMN_NAME
        }
        constraintrow.set(6,"");                                            // PK_TABLE_CAT
        constraintrow.set(7,"");                                            // PK_TABLE_SCHEM
        constraintrow.set(8,constraint.getChildTableName());                // FK_TABLE_NAME
        constraintrow.set(9,constraint.getName());                          // FK_NAME
        //Assuming only one column exists per FK name
        if (constraint.getChildTableColumns() != null){
            constraintrow.set(10,((Selectable)constraint.getChildTableColumns().get(0)).getName());  // FK_COLUMN_NAME
        }
        constraintrow.set(11,constraint.getOnUpdateActionType());            // UPDATE_RULE
        constraintrow.set(12,constraint.getOnDeleteActionType());            // DELETE_RULE
        constraintrow.set(13,constraint.SETNULL);                            // DEFERRABILITY
        return constraintrow;
    }
    
    private boolean checkIfKeyConstraintAdded(String keyname){
        boolean available = false;
        for (int i=0; i < addedKeys.size();i++){
            if (keyname.equals(addedKeys.get(i))) {
                available = true;
                break;
            }
        }
        return available;
    }
    
    public void columnAdded(ColumnEvent event) throws AxionException {
    }
    
    public void rowInserted(RowEvent event) throws AxionException {
    }
    
    public void rowDeleted(RowEvent event) throws AxionException {
    }
    
    public void rowUpdated(RowEvent event) throws AxionException {
    }
    
    public void constraintAdded(ConstraintEvent event) throws AxionException {
    }
    
    public void constraintRemoved(ConstraintEvent event) throws AxionException {
    }
    
    public void tableAdded(DatabaseModifiedEvent e) {
        Iterator i = e.getTable().getConstraints();
        while(i.hasNext()){
            Constraint c = (Constraint)i.next();
            try {
                if(!checkIfKeyConstraintAdded(c.getName())){
                    ArrayList rowlist = createPkFkConstraintRows(c, e.getTable().getName());
                    //Add Constraints in the Keys Table
                    for (int j=0; j < rowlist.size(); j++){
                        _db.getTable("AXION_KEYS").addRow((Row)rowlist.get(j));
                    }
                    this.addedKeys.add(c.getName());
                }
            } catch (AxionException ex) {
                _log.severe("Unable to Enter Constraint into AXION_KEYS : " + ex);
            }
        }
    }
    
}
