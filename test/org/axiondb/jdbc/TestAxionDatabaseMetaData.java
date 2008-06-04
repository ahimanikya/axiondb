/*
 * $Id: TestAxionDatabaseMetaData.java,v 1.3 2008/05/14 10:08:17 jawed Exp $
 * =======================================================================
 * Copyright (c) 2003-2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.Index;
import org.axiondb.IndexFactory;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableFactory;
import org.axiondb.TableIdentifier;
import org.axiondb.TransactionManager;
import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.functional.AbstractFunctionalTest;
import org.axiondb.functions.ConcreteFunction;

/**
 * @version $Revision: 1.3 $ $Date: 2008/05/14 10:08:17 $
 * @author Rod Waldhoff
 * @author Ahimanikya Satapathy
 */
public class TestAxionDatabaseMetaData extends AbstractFunctionalTest {

    private DatabaseMetaData _meta = null;

    public TestAxionDatabaseMetaData(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestAxionDatabaseMetaData.class);
    }

    public void setUp() throws Exception {
        super.setUp();
        _meta = _conn.getMetaData();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        _meta = null;
    }

    public void testGetNumericFunctions() throws Exception {
        try {
            _meta.getNumericFunctions();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetSystemFunctions() throws Exception {
        try {
            _meta.getSystemFunctions();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetSQLKeywords() throws Exception {
        try {
            _meta.getSQLKeywords();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetSearchStringEscape() throws Exception {
        try {
           assertEquals(_meta.getSearchStringEscape(), "\"");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetStringFunctions() throws Exception {
        try {
            _meta.getStringFunctions();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGettTimeDateFunctions() throws Exception {
        try {
            _meta.getTimeDateFunctions();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetExtraNameCharacters() throws Exception {
        try {
            _meta.getExtraNameCharacters();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetSchemaTerm() throws Exception {
        try {
            _meta.getSchemaTerm();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetProcedureTerm() throws Exception {
        try {
            _meta.getProcedureTerm();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetCatalogTerm() throws Exception {
        try {
            _meta.getCatalogTerm();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testIsCatalogAtStart() throws Exception {
        try {
            _meta.isCatalogAtStart();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetCatalogSeparator() throws Exception {
        try {
            _meta.getCatalogSeparator();
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetProcedures() throws Exception {
        try {
            _meta.getProcedures(null, null, null);
            // fail("Expected SQLException");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void testGetProcedureColumns() throws Exception {
        try {
            _meta.getProcedureColumns(null, null, null, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetColumnPrivileges() throws Exception {
        try {
            _meta.getColumnPrivileges(null, null, null, null);
            // fail("Expected SQLException"); 28-Nov-2007:Commented as implementation is there now.
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void testGetTablePrivileges() throws Exception {
        try {
            _meta.getTablePrivileges(null, null, null);
            // fail("Expected SQLException"); 28-Nov-2007:Commented as implementation is there now.
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void testGetBestRowIdentifier() throws Exception {
        try {
            _meta.getBestRowIdentifier(null, null, null, 0, false);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetVersionColumns() throws Exception {
        try {
            _meta.getVersionColumns(null, null, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetPrimaryKeys() throws Exception {
        try {
            _meta.getPrimaryKeys(null, null, null);
            // fail("Expected SQLException"); 28-Nov-2007:Commented as implementation is there now.
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void testGetImportedKeys() throws Exception {
        try {
            _meta.getImportedKeys(null, null, null);
           // fail("Expected SQLException"); 28-Nov-2007:Commented as implementation is there now.
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void testGetExportedKeys() throws Exception {
        try {
            _meta.getExportedKeys(null, null, null);
            //fail("Expected SQLException"); 28-Nov-2007:Commented as implementation is there now.
        } catch (SQLException e) {
            fail("Expected SQLException");
        }
    }

    public void testGetCrossReference() throws Exception {
        try {
            _meta.getCrossReference(null, null, null, null, null, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testSupportsResultSetConcurrency() throws Exception {
        assertTrue(_meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertTrue(_meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
        assertTrue(_meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertTrue(_meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));
        assertFalse(_meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertFalse(_meta.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));

        try {
            _meta.supportsResultSetConcurrency(0, 0);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testChangeVisibility() throws Exception {
        assertTrue(_meta.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(_meta.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(_meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(_meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertTrue(_meta.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(!_meta.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(!_meta.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(!_meta.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(_meta.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(_meta.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(_meta.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertTrue(!_meta.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(!_meta.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(!_meta.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(_meta.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(_meta.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(_meta.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
    }

    public void testGetUDTs() throws Exception {
        try {
            _meta.getUDTs(null, null, null, null);
            fail("Expected SQLException");
        } catch (SQLException e) {
            // expected
        }
    }

    public void testGetCatalogs() throws Exception {
        assertNotNull(_meta.getCatalogs());
    }

    public void testGetSchemas() throws Exception {
        assertNotNull(_meta.getSchemas());
    }

    public void testGetTableTypes() throws Exception {
        assertNotNull(_meta.getTableTypes());
    }

    public void testGetTypeInfo() throws Exception {
        assertNotNull(_meta.getTypeInfo());
    }

    public void testGetSuperTables() throws Exception {
        assertNotNull(_meta.getSuperTables(null, null, null));
    }

    public void testGetSuperTypes() throws Exception {
        assertNotNull(_meta.getSuperTypes(null, null, null));
    }

    public void testGetColumns() throws Exception {
        assertNotNull(_meta.getColumns(null, null, null, null));
        assertNotNull(_meta.getColumns("", "%", "%", "%"));
        assertNotNull(_meta.getColumns("x", "x", "x", "x"));
    }

    public void testGetTables() throws Exception {
        DatabaseMetaData meta = _conn.getMetaData();
        {
            ResultSet rset = meta.getTables(null, null, "FOO", new String[] { "TABLE"});
            assertNotNull(rset);
            assertTrue(!rset.next());
            rset.close();
        }

        _stmt.execute("create table foo ( id integer )");
        {
            ResultSet rset = meta.getTables(null, null, "FOO", new String[] { "TABLE"});
            assertNotNull(rset);
            assertTrue(rset.next());
            assertTrue(!rset.next());
            rset.close();
        }
    }

    public void testGetConnection() throws Exception {
        assertEquals(_conn, _meta.getConnection());
    }

    public void testGetUserName() throws Exception {
        assertNull(_meta.getUserName());
    }

    public void testGetDefaultTransactionIsolation() throws Exception {
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, _meta.getDefaultTransactionIsolation());
    }

    public void testGetURL() throws Exception {
        assertEquals(getConnectString(), _meta.getURL());
    }

    public void testReadOnly() throws Exception {
        MockDatabase mockdb = new MockDatabase();
        AxionConnection conn = new AxionConnection(mockdb);
        DatabaseMetaData meta = new AxionDatabaseMetaData(conn, mockdb);
        mockdb.setReadOnly(true);
        assertTrue(meta.isReadOnly());
        mockdb.setReadOnly(false);
        assertTrue(!meta.isReadOnly());
    }

    public void testSupportsResultSetType() throws Exception {
        assertTrue(_meta.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(_meta.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
        assertFalse(_meta.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
    }

    public void testOther() throws Exception {
        assertTrue(!_meta.usesLocalFiles()); // what exactly does this mean?
        assertTrue(!_meta.usesLocalFilePerTable()); // what exactly does this mean?
    }

    public void testVendorInfo() throws Exception {
        assertNotNull(_meta.getDatabaseProductName());
        assertNotNull(_meta.getDatabaseProductVersion());
        assertNotNull(_meta.getDriverName());
        assertNotNull(_meta.getDriverVersion());
        assertEquals(0, _meta.getDriverMajorVersion());
        assertEquals(3, _meta.getDriverMinorVersion());
        assertEquals(0, _meta.getDatabaseMajorVersion());
        assertEquals(3, _meta.getDatabaseMinorVersion());
    }

    public void testNotSupportedFeatures() throws Exception {
        assertTrue(!_meta.supportsSelectForUpdate());
        assertTrue(!_meta.supportsSubqueriesInQuantifieds()); // ?
        assertTrue(!_meta.supportsConvert(1, 1));
        assertTrue(!_meta.supportsMultipleResultSets());
        assertTrue(!_meta.supportsGetGeneratedKeys());
        assertTrue(!_meta.supportsCoreSQLGrammar());
        assertTrue(!_meta.supportsFullOuterJoins());
        assertTrue(!_meta.supportsIntegrityEnhancementFacility());
        assertTrue(!_meta.supportsANSI92IntermediateSQL());
        assertTrue(!_meta.supportsSchemasInPrivilegeDefinitions());
        assertTrue(!_meta.supportsExtendedSQLGrammar());
        assertTrue(!_meta.supportsSchemasInTableDefinitions());

        // per the javadoc, this refers to CallableStatements,
        // which we don't support at all
        assertTrue(!_meta.supportsMultipleOpenResults());

        // per the javadoc, this refers to CallableStatements,
        // which we don't support at all
        assertTrue(!_meta.supportsNamedParameters());

        assertTrue(!_meta.supportsSavepoints());
        assertTrue(!_meta.supportsStatementPooling());
    }

    public void testSupportedFeatures() throws Exception {
        assertTrue(_meta.supportsBatchUpdates());
        assertTrue(_meta.allTablesAreSelectable());
        assertTrue(_meta.supportsTableCorrelationNames());
        assertTrue(_meta.supportsDifferentTableCorrelationNames());
        assertTrue(_meta.supportsColumnAliasing());
        assertTrue(_meta.supportsOuterJoins());
        assertTrue(_meta.supportsLimitedOuterJoins());
        assertTrue(_meta.supportsMinimumSQLGrammar());
        assertTrue(_meta.nullPlusNonNullIsNull());
        assertTrue(_meta.supportsNonNullableColumns());
        assertTrue(_meta.supportsSubqueriesInComparisons());
        assertTrue(_meta.supportsSubqueriesInExists());
        assertTrue(_meta.supportsSubqueriesInIns());
        assertTrue(_meta.supportsConvert());
        assertTrue(_meta.supportsCorrelatedSubqueries());
        assertTrue(_meta.supportsANSI92EntryLevelSQL());
        assertTrue(_meta.supportsLikeEscapeClause());
        assertTrue(_meta.supportsAlterTableWithDropColumn());
        assertTrue(_meta.supportsAlterTableWithAddColumn());
        assertTrue(_meta.supportsPositionedDelete());
        assertTrue(_meta.supportsPositionedUpdate());
    }

    public void testTransactions() throws Exception {
        assertTrue(_meta.supportsTransactions());

        assertTrue(_meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        assertTrue(!_meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        assertTrue(!_meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        assertTrue(!_meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        assertTrue(!_meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        assertTrue(!_meta.supportsTransactionIsolationLevel(-12345));

        assertTrue(_meta.supportsMultipleTransactions());
        assertTrue(!_meta.supportsOpenCursorsAcrossCommit());
        assertTrue(!_meta.supportsOpenCursorsAcrossRollback());
        assertTrue(_meta.supportsOpenStatementsAcrossCommit());
        assertTrue(_meta.supportsOpenStatementsAcrossRollback());

        assertTrue(_meta.supportsDataManipulationTransactionsOnly());
        assertTrue(!_meta.supportsDataDefinitionAndDataManipulationTransactions());
        assertTrue(!_meta.dataDefinitionCausesTransactionCommit());
        assertTrue(!_meta.dataDefinitionIgnoredInTransactions());

        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, _meta.getResultSetHoldability());
        assertTrue(_meta.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertTrue(!_meta.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    public void testProcedures() throws Exception {
        assertTrue(!_meta.supportsStoredProcedures());
        assertTrue(!_meta.allProceduresAreCallable());
        assertEquals(0, _meta.getMaxProcedureNameLength());
    }

    public void testSchemas() throws Exception {
        assertEquals(0, _meta.getMaxSchemaNameLength());
        assertTrue(!_meta.supportsSchemasInDataManipulation());
        assertTrue(!_meta.supportsSchemasInProcedureCalls());
        assertTrue(!_meta.supportsSchemasInIndexDefinitions());
    }

    public void testCatalogs() throws Exception {
        assertEquals(0, _meta.getMaxCatalogNameLength());
        assertTrue(!_meta.supportsCatalogsInDataManipulation());
        assertTrue(!_meta.supportsCatalogsInProcedureCalls());
        assertTrue(!_meta.supportsCatalogsInTableDefinitions());
        assertTrue(!_meta.supportsCatalogsInIndexDefinitions());
        assertTrue(!_meta.supportsCatalogsInPrivilegeDefinitions());
    }

    public void testOrderBy() throws Exception {
        assertEquals(Integer.MAX_VALUE, _meta.getMaxColumnsInOrderBy());
        assertTrue(_meta.supportsOrderByUnrelated());
        assertTrue(_meta.supportsExpressionsInOrderBy());
    }

    public void testGroupBy() throws Exception {
        assertEquals(0, _meta.getMaxColumnsInGroupBy());
        assertTrue(_meta.supportsGroupBy());
        assertTrue(_meta.supportsGroupByBeyondSelect());
        assertTrue(_meta.supportsGroupByUnrelated());
    }

    public void testUnion() throws Exception {
        assertTrue(!_meta.supportsUnion());
        assertTrue(!_meta.supportsUnionAll());
    }

    public void testLimits() throws Exception {
        assertEquals(0, _meta.getMaxRowSize());
        assertEquals(0, _meta.getMaxStatementLength());

        assertEquals(0, _meta.getMaxConnections());
        assertEquals(0, _meta.getMaxStatements());
        assertEquals(Integer.MAX_VALUE, _meta.getMaxColumnNameLength());
        assertEquals(Integer.MAX_VALUE, _meta.getMaxTableNameLength());
        assertEquals(0, _meta.getMaxUserNameLength());
        assertEquals(0, _meta.getMaxCursorNameLength());

        assertEquals(0, _meta.getMaxBinaryLiteralLength());
        assertEquals(0, _meta.getMaxCharLiteralLength());

        assertEquals(0, _meta.getMaxIndexLength());

        assertEquals(1, _meta.getMaxColumnsInIndex());
        assertEquals(Integer.MAX_VALUE, _meta.getMaxTablesInSelect());
        assertEquals(Integer.MAX_VALUE, _meta.getMaxColumnsInSelect());
        assertEquals(Integer.MAX_VALUE, _meta.getMaxColumnsInTable());

        assertTrue(!_meta.doesMaxRowSizeIncludeBlobs());
    }

    public void testIdentifiers() throws Exception {
        assertTrue(_meta.storesUpperCaseIdentifiers());
        assertTrue(_meta.storesUpperCaseQuotedIdentifiers());
        assertTrue(_meta.storesUpperCaseIdentifiers() != _meta.storesLowerCaseIdentifiers());
        assertTrue(_meta.storesUpperCaseIdentifiers() != _meta.storesMixedCaseIdentifiers());

        assertTrue(!_meta.supportsMixedCaseIdentifiers());
        assertTrue(!_meta.supportsMixedCaseQuotedIdentifiers());
        assertTrue(!_meta.storesMixedCaseQuotedIdentifiers());
        assertTrue(!_meta.storesLowerCaseQuotedIdentifiers());
        assertEquals("\"", _meta.getIdentifierQuoteString());
    }

    public void testNullSorting() throws Exception {
        assertTrue(_meta.nullsAreSortedHigh());
        assertTrue(_meta.nullsAreSortedHigh() != _meta.nullsAreSortedLow());
        assertTrue(_meta.nullsAreSortedHigh() != _meta.nullsAreSortedAtStart());
        assertTrue(_meta.nullsAreSortedHigh() != _meta.nullsAreSortedAtEnd());
    }

    private static class MockDatabase implements Database {

        public void defrag() throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public int defragTable(String tableName) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void dropDatabaseLink(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public boolean hasDatabaseLink(String name) throws AxionException {
            return false;
        }

        public void setReadOnly(boolean readOnly) {
            _readOnly = readOnly;
        }

        // --------------------------------------------------------------------
        public void addDatabaseModificationListener(DatabaseModificationListener l) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public boolean hasIndex(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void dropIndex(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void addIndex(Index index, Table table) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void addIndex(Index index, Table table, boolean doPopulate) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void addTable(Table table) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void checkpoint() throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void createSequence(Sequence seq) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void dropSequence(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void createDatabaseLink(DatabaseLink server) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public DatabaseLink getDatabaseLink(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void dropTable(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public List getDatabaseModificationListeners() {
            throw new UnsupportedOperationException("Not implemented");
        }

        public DataType getDataType(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public IndexFactory getIndexFactory(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public String getName() {
            throw new UnsupportedOperationException("Not implemented");
        }

        public Object getGlobalVariable(String key) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public ConcreteFunction getFunction(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public Sequence getSequence(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public Table getTable(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public Table getTable(TableIdentifier table) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public boolean hasSequence(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public boolean hasTable(String name) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public boolean hasTable(TableIdentifier table) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public List getDependentViews(String tableName) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void dropDependentViews(List views) throws AxionException{
            throw new UnsupportedOperationException("Not implemented");
        }

        public void dropDependentExternalDBTable(List tables) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public List getDependentExternalDBTable(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }


        public File getDBDirectory() {
            throw new UnsupportedOperationException("Not implemented");
        }

        public TableFactory getTableFactory(String name) {
            throw new UnsupportedOperationException("Not implemented");
        }

        public TransactionManager getTransactionManager() {
            throw new UnsupportedOperationException("Not implemented");
        }

        public boolean isReadOnly() {
            return _readOnly;
        }

        public void migrate(int version) throws AxionException{
            throw new UnsupportedOperationException("Not implemented");
        }

        public void remount(File newdir) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void renameTable(String oldName, String newName) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void renameTable(String oldName, String newName, Properties newTableProp) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void shutdown() throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        public void tableAltered(Table t) throws AxionException {
            throw new UnsupportedOperationException("Not implemented");
        }

        //-----------------------------------------------------------

        private boolean _readOnly = false;

    }

}