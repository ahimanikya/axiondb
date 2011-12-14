/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.tables;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.primitives.IntCollection;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Constraint;
import org.axiondb.Database;
import org.axiondb.Index;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowDecorator;
import org.axiondb.RowIterator;
import org.axiondb.RowSource;
import org.axiondb.Selectable;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactableTable;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.SnapshotIsolationTransaction;
import org.axiondb.engine.TransactableTableImpl;
import org.axiondb.engine.commands.AxionQueryContext;
import org.axiondb.engine.commands.SelectCommand;
import org.axiondb.engine.commands.SubSelectCommand;
import org.axiondb.engine.rowiterators.FilteringRowIterator;
import org.axiondb.engine.rowiterators.RowViewRowIterator;
import org.axiondb.engine.rowiterators.UnmodifiableRowIterator;
import org.axiondb.event.BaseTableModificationPublisher;
import org.axiondb.event.ColumnEvent;
import org.axiondb.functions.AndFunction;
import org.axiondb.functions.ConcreteFunction;
import org.axiondb.functions.EqualFunction;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.parser.AxionSqlParser;
import org.axiondb.util.StringIdentifierGenerator;
import org.axiondb.util.ValuePool;

/**
 * A sub-query view {@link Table}.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class TableView extends BaseTableModificationPublisher implements Table {

    public TableView(Database db, String name, String type, SubSelectCommand subSelectCmd)
            throws AxionException {
        setType(type);
        _name = (name == null) ? generateName() : name.toUpperCase();
        _subSelectCmd = subSelectCmd;
        _db = db;

        _isDiskDb = false;
        if ((name != null) && (db instanceof SnapshotIsolationTransaction)) {
            if (((SnapshotIsolationTransaction) db).getOpenOnTransaction() instanceof DiskDatabase) {
                _isDiskDb = true;
            }
        }
        // Or
        if ((name != null) && (db instanceof DiskDatabase)) {
            _isDiskDb = true;
        }

        if (_isDiskDb) {
            createOrLoadMetaFile(_name, db);
            File typefile = new File(_dir, _name + ".TYPE");
            writeNameToFile(typefile, new TableViewFactory());
        }

        init();
    }

    public TableView(Database db, String name, SubSelectCommand subSelectCmd) throws AxionException {
        this(db, name, SUBQUERY, subSelectCmd);
    }

    public TableView(Database db, String name) throws AxionException {
        setType(VIEW);
        _isDiskDb = true;
        _name = name.toUpperCase();
        _db = db;

        createOrLoadMetaFile(name, db);

        AxionSqlParser parser = new AxionSqlParser();
        SelectCommand selectCmd = (SelectCommand) parser.parse(_subQuery);
        _subSelectCmd = new SubSelectCommand(selectCmd.getQueryContext());

        init();
    }

    private void init() throws AxionException {
        _trueColumns = new ArrayList();
        AxionQueryContext ctx = _subSelectCmd.getQueryContext();
        for (int i = 0, I = ctx.getSelectCount(); i < I; i++) {
            if (ctx.getSelect(i) instanceof ColumnIdentifier) {
                ColumnIdentifier col = (ColumnIdentifier) ctx.getSelect(i);
                _trueColumns.add(new ColumnIdentifier(col.getTableIdentifier(), col.getName(), col
                    .getAlias(), col.getDataType()));
            }
        }

        _subSelectCmd.makeRowIterator(_db, true);
        _select = _subSelectCmd.getQueryContext().getResolvedSelect();
        _srcColIdToFieldMap = _subSelectCmd.getColumnIdToFieldMap();
        checkAmbiguity(_srcColIdToFieldMap);

        for (int i = 0, I = getColumnCount(); i < I; i++) {
            publishEvent(new ColumnEvent(this, getColumn(i)));
        }
    }

    public void setSubQuery(String query) throws AxionException {
        _subQuery = query;
        if (_isDiskDb) {
            writeMetaFile(getMetaFile());
        }
    }

    public void populateIndex(Index index) throws AxionException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public int getNextRowId() {
        return -1;
    }

    public void freeRowId(int id) {
    }

    public int getRowCount() {
        int count = 0;
        try {
            for (RowIterator itr = getRowIterator(); itr.hasNext(); itr.next()) {
                count++;
            }
        } catch (AxionException aex) {
            count = 0;
        }
        return count;
    }

    public Row getRow(int id) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void applyInserts(RowCollection rows) throws AxionException {
        throw new UnsupportedOperationException("Can't add row to a view");
    }

    public void applyDeletes(IntCollection rowids) throws AxionException {
        throw new UnsupportedOperationException("Can't delete row from a view");
    }

    public void applyUpdates(RowCollection rows) throws AxionException {
        throw new UnsupportedOperationException("Can't update row of a view");
    }

    protected RowIterator getRowIterator() throws AxionException {
        RowViewRowIterator rowIterator = new RowViewRowIterator(_subSelectCmd.makeRowIterator(_db, true),
            _srcColIdToFieldMap, getCanonicalIdentifiers(_select));
        return rowIterator;
    }

    public RowIterator getRowIterator(boolean readOnly) throws AxionException {
        if (readOnly) {
            return UnmodifiableRowIterator.wrap(getRowIterator());
        }
        return getRowIterator();
    }

    public void addRow(Row row) throws AxionException {
        throw new UnsupportedOperationException("Can't add row to a view");
    }

    public void updateRow(Row oldrow, Row newrow) throws AxionException {
        throw new UnsupportedOperationException("Can't update row of a view");
    }
    
    public void deleteRow(Row oldrow) throws AxionException {
        throw new UnsupportedOperationException("Can't delete row of a view");
    }

    public RowDecorator buildRowDecorator() {
        RowDecorator dec = null;
        {
            int I = getColumnCount();
            Map map = new HashMap(I);
            for (int i = 0; i < I; i++) {
                map.put(getColumn(i), ValuePool.getInt(i));
            }
            dec = new RowDecorator(map);
        }
        return dec;
    }

    public String toString() {
        return getName();
    }

    public final String getName() {
        return _name;
    }

    public final String getType() {
        return _type;
    }

    public void setType(String type) {
        _type = type;
    }

    public void addConstraint(Constraint constraint) throws AxionException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Constraint removeConstraint(String name) {
        throw new UnsupportedOperationException("Not implemented");
    }
    
    public Constraint getConstraint(String name) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Iterator getConstraints() {
        return Collections.EMPTY_LIST.iterator();
    }

    /**
     * check if unique constraint exists on a column
     * 
     * @param columnName name of the columm
     * @return true if uniqueConstraint exists on the column
     */
    public boolean isUniqueConstraintExists(String columnName) {
        return false;
    }

    /**
     * check if primary constraint exists on a column
     * 
     * @param ColumnName name of the column
     * @return if PrimaryKeyConstraint exists on the column
     */
    public final boolean isPrimaryKeyConstraintExists(String columnName) {
        return false;
    }

    public void addIndex(Index index) throws AxionException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void removeIndex(Index index) throws AxionException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Index getIndexForColumn(Column column) {
        return null;
    }

    public final boolean isColumnIndexed(Column column) {
        return false;
    }

    public RowIterator getMatchingRows(List selectables, List values, boolean readOnly) throws AxionException {
        if (null == selectables || selectables.isEmpty()) {
            return getRowIterator(readOnly);
        }

        Selectable root = null;
        for (int i = 0, I = selectables.size(); i < I; i++) {
            Selectable sel = (Selectable) selectables.get(i);
            Object val = values.get(i);

            EqualFunction function = new EqualFunction();
            function.addArgument(sel);
            function.addArgument(new Literal(val));
            if (null == root) {
                root = function;
            } else {
                AndFunction fn = new AndFunction();
                fn.addArgument(root);
                fn.addArgument(function);
                root = fn;
            }
        }
        return new FilteringRowIterator(getRowIterator(readOnly), makeRowDecorator(), root);
    }
    
    public RowIterator getIndexedRows(Selectable node, boolean readOnly) throws AxionException {
        return getIndexedRows(this, node, readOnly);
    }

    public RowIterator getIndexedRows(RowSource source, Selectable node, boolean readOnly) throws AxionException {
        return null;
    }

    public void addColumn(Column col) throws AxionException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public boolean hasColumn(ColumnIdentifier id) {
        boolean result = false;
        for (int i = 0, I = _select.size();  i <  I; i++) {
            Object sel = _select.get(i);
            if (sel instanceof ColumnIdentifier) {
                ColumnIdentifier col = (ColumnIdentifier) sel;
                if (id.getName().equals(getResolvedColumnName(col))) {
                    result = true;
                    break;
                }
            } else {
                if (getResolvedColumnName(id).equals(getResolvedColumnName(sel))) {
                    result = true;
                    break;
                }
            }
        }
        return result;
    }

    public final Column getColumn(int index) {
        Selectable sel = (Selectable) _select.get(index);
        return new Column(getResolvedColumnName(sel), sel.getDataType());
    }

    public final Column getColumn(String name) {
        return getColumn(getColumnIndex(name));
    }

    public int getColumnIndex(String name) {
        for (int i = 0, I = _select.size();  i <  I; i++) {
            Object sel = _select.get(i);
            if (getResolvedColumnName(sel).equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column " + name + " not found.");
    }

    public final List getColumnIdentifiers() {
        return getColumnIdentifierList(new TableIdentifier(_name));
    }

    public List getColumnIdentifierList(TableIdentifier table) {
        if (table != null && !getName().equals(table.getTableName())) {
            return Collections.EMPTY_LIST;
        }

        List result = new ArrayList();
        for (int i = 0, I = _select.size();  i <  I; i++) {
            Selectable sel = (Selectable) _select.get(i);
            result.add(new ColumnIdentifier(table, getResolvedColumnName(sel), sel.getAlias(), sel
                .getDataType()));
        }
        return result;
    }

    public final int getColumnCount() {
        return _select.size();
    }

    public void drop() throws AxionException {
        if (_isDiskDb && !deleteFile(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table "
                + getName());
        }
    }

    protected boolean deleteFile(File file) {
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (int i = 0; i < files.length; i++) {
                    deleteFile(files[i]);
                }
            }
            if (!file.delete()) {
                return false;
            }
            return true;
        }
        return true;
    }

    public void remount(File dir, boolean datafilesonly) throws AxionException {
    }

    public void rename(String oldName, String newName) throws AxionException {
        _name = newName.toUpperCase();
    }

    public void shutdown() throws AxionException {
        _select = null;
        _srcColIdToFieldMap = null;
        _subSelectCmd = null;
        _db = null;
    }

    public void checkpoint() throws AxionException {
    }

    public void setSequence(Sequence seq) throws AxionException {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public Sequence getSequence() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public void truncate() throws AxionException {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public RowDecorator makeRowDecorator() {
        if (null == _colIndexToColIdMap) {
            Map map = new HashMap();
            List colids = getColumnIdentifiers();
            for (int i = 0, I = colids.size(); i < I; i++) {
                map.put(colids.get(i), ValuePool.getInt(i));
            }
            _colIndexToColIdMap = map;
        }
        return new RowDecorator(_colIndexToColIdMap);
    }

    public TransactableTable makeTransactableTable() {
        return new TransactableTableImpl(this);
    }
    
    public void migrate() throws AxionException{
    }

    public final Iterator getIndices() {
        return Collections.EMPTY_LIST.iterator();
    }
    
    public Iterator getTables() {
        return Arrays.asList(_subSelectCmd.getQueryContext().getTables()).iterator();
    }

    public final boolean hasIndex(String name) {
        return false;
    }

    private void checkAmbiguity(Map colIdToFieldMap) throws AxionException {
        Set colidset = new HashSet();
        for (int i = 0, I = _select.size();  i <  I; i++) {
            Object col = _select.get(i);
            String colname = getResolvedColumnName(col);

            // problem for cases like... (select * from x,y) as S
            // here if x and y have column ID, then S.ID is ambiguous
            if (colidset.contains(colname) && !(col instanceof Literal)) {
                throw new AxionException(42706);
            }
            colidset.add(colname);
        }

        // if we have the ambigious unqualified column name this should catch it
        for (int i = 0, I =_trueColumns.size(); i < I; i++) {
            ColumnIdentifier truecol = (ColumnIdentifier) _trueColumns.get(i);
            if (truecol.getTableName() == null) {
                checkUnqualifiedColumnAmbiguity(colIdToFieldMap, truecol.getName());
            }
        }
    }

    private void checkUnqualifiedColumnAmbiguity(Map colIdToFieldMap, String truecolname) throws AxionException{
        Set truecolset = new HashSet();
        Iterator it = colIdToFieldMap.keySet().iterator();
        while (it.hasNext()) {
            Object obj = it.next();
            if (obj instanceof ColumnIdentifier) {
                ColumnIdentifier col = (ColumnIdentifier) obj;
                String colname = col.getName();
                if (truecolname.equals(colname)) {
                    if (!truecolset.contains(colname)) {
                        truecolset.add(colname);
                    } else {
                        throw new AxionException(42705);
                    }
                }
            }
        }
    }

    private List getCanonicalIdentifiers(List selectedCol) {
        List colids = new ArrayList();
        for (int i = 0, I = selectedCol.size(); i < I; i++) {
            Object sel = selectedCol.get(i);
            if (sel instanceof ColumnIdentifier) {
                ColumnIdentifier colid = (ColumnIdentifier) sel;
                colids.add(colid.getCanonicalIdentifier());
            } else {
                colids.add(sel);
            }
        }
        return colids;
    }

    private String getResolvedColumnName(Object obj) {
        String colName = null;
        if (obj instanceof ColumnIdentifier) {
            ColumnIdentifier col = (ColumnIdentifier) obj;
            colName = col.getAlias();
            if (colName == null) {
                colName = col.getName();
            }
        } else if (obj instanceof ConcreteFunction) {
            ConcreteFunction fn = (ConcreteFunction) obj;
            colName = fn.getLabel();
        } else if (obj instanceof SubSelectCommand) {
            SubSelectCommand sq = (SubSelectCommand) obj;
            colName = sq.getName();
        } else if (obj instanceof Literal) {
            Literal lit = (Literal) obj;
            colName = lit.getName();
        }
        return colName;
    }

    private static String generateName() {
        return StringIdentifierGenerator.INSTANCE.next16DigitIdentifier("VIEW");
    }

    private void createOrLoadMetaFile(String name, Database db) throws AxionException {
        _dir = new File(db.getDBDirectory(), name);

        if (!_dir.exists()) {
            if (!_dir.mkdirs()) {
                throw new AxionException("Unable to create directory \"" + _dir.toString()
                    + "\" for view \"" + name + "\".");
            }
        }

        if (getMetaFile().exists()) {
            parseMetaFile(getMetaFile());
        }
    }

    private void parseMetaFile(File file) throws AxionException {
        ObjectInputStream in = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            in = fs.openObjectInputSteam(file);
            _name = in.readUTF();
            _subQuery = in.readUTF();

        } catch (IOException e) {
            throw new AxionException("Unable to parse meta file " + file + " for table "
                + getName(), e);
        } finally {
            fs.closeInputStream(in);
        }
    }

    private void writeMetaFile(File file) throws AxionException {
        ObjectOutputStream out = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            out = fs.createObjectOutputSteam(file);
            out.writeUTF(_name);
            out.writeUTF(_subQuery);
            out.flush();
        } catch (IOException e) {
            throw new AxionException("Unable to write meta file " + file + " for table "
                + getName(), e);
        } finally {
            fs.closeOutputStream(out);
        }
    }

    private File getMetaFile() {
        if (null == _metaFile) {
            _metaFile = new File(getRootDir(), getName() + ".META");
        }
        return _metaFile;
    }

    private final File getRootDir() {
        return _dir;
    }

    private void writeNameToFile(File file, Object obj) throws AxionException {
        ObjectOutputStream out = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            String name = obj.getClass().getName();
            out = fs.createObjectOutputSteam(file);
            out.writeUTF(name);
            out.flush();
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            fs.closeOutputStream(out);
        }
    }

    private String _name;
    private String _type;
    private String _subQuery;
    private List _select;
    private List _trueColumns;
    private Map _colIndexToColIdMap;
    private Map _srcColIdToFieldMap;
    private boolean _isDiskDb = false;
    private SubSelectCommand _subSelectCmd;
    private Database _db;

    private File _dir;
    private File _metaFile;

    public static String VIEW = "VIEW";
    public static String SUBQUERY = "SUB QUERY";
}
