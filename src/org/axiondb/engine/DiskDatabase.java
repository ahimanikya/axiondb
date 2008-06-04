/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.AxionException;
import org.axiondb.DatabaseLink;
import org.axiondb.Sequence;
import org.axiondb.Table;
import org.axiondb.TableFactory;
import org.axiondb.engine.commands.AlterTableCommand;
import org.axiondb.engine.tables.BaseDiskTable;
import org.axiondb.engine.tables.MemoryTable;
import org.axiondb.engine.tables.TableViewFactory;
import org.axiondb.io.FileUtil;

/**
 * A disk-resident {@link org.axiondb.Database}.
 *
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Morgan Delagrange
 * @author Ahimanikya Satapathy
 */
public class DiskDatabase extends BaseDatabase {
    
    //------------------------------------------------------------- Constructors
    
    public DiskDatabase(File dbDir) throws AxionException {
        this(dbDir.getName(), dbDir);
    }
    
    public DiskDatabase(String name, File dbDir) throws AxionException {
        this(name, dbDir, null);
    }
    
    public DiskDatabase(String name, File dbDir, Properties props) throws AxionException {
        super(name);
        if (null == dbDir) {
            throw new AxionException("Database directory required.");
        }
        
        _dbDir = dbDir;
        _log.log(Level.FINE,"Constructing disk-based database in " + dbDir);
        
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            throw new AxionException("Database directory \"" + dbDir + "\" could not be created or is not a directory.");
        }
        
        props = loadProperties(dbDir, props);
        obtainLockFile(props);
        obtainDBVersion();
        migrate(_dbVersion);
        
        createMetaDataTables();
        
        // TODO: Seems like this should go after loadProperties(dbDir,props), but that
        // breaks a unit test
        loadProperties(props);
        loadDBLinks();
        loadTables(_dbDir);
        loadSequences();
        
        if (!isReadOnly()) {
            writeDbVersion();
        }
        _log.log(Level.FINE,"Disk-based database construction successful");
    }
    
    private void writeDbVersion() throws AxionException {
        File verFile = getDbFileName(".VER");
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new FileOutputStream(verFile));
            out.writeInt(AXION_DB_VERSION);
            out.flush();
        } catch (IOException e) {
            String msg = "Unable to persist version file";
            _log.log(Level.SEVERE,msg, e);
            throw new AxionException(msg);
        } finally {
            closeOutputStream(out);
        }
    }
    
    protected File getDbFileName(String extension) {
        return new File(_dbDir, getName().toUpperCase() + extension);
    }
    
    @Override
    public void checkpoint() throws AxionException {
        super.checkpoint();
        if (getSequenceCount() != 0) {
            File seqFile = getDbFileName(".SEQ");
            DataOutputStream out = null;
            try {
                out = new DataOutputStream(new FileOutputStream(seqFile));
                out.writeInt(getSequenceCount());
                
                for (Iterator i = getSequences(); i.hasNext();) {
                    Sequence cur = (Sequence) (i.next());
                    cur.write(out);
                }
                out.flush();
            } catch (IOException e) {
                String msg = "Unable to persist sequence file";
                _log.log(Level.SEVERE,msg, e);
                throw new AxionException(msg);
            } finally {
                closeOutputStream(out);
            }
        }
    }
    
    private void closeOutputStream(DataOutputStream out) {
        if (out != null) {
            try {
                out.close();
            } catch (Exception e) {
            }
        }
    }
    
    @Override
    public void createSequence(Sequence seq) throws AxionException {
        super.createSequence(seq);
        checkpoint();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void defrag() throws AxionException {
        checkpoint();
        
        // Copying to local List to avoid ConcurrentModificationException
        Iterator i = getTables();
        List tablesToDefrag = new ArrayList();
        while (i.hasNext()) {
            Table table = (Table) (i.next());
            if (table instanceof BaseDiskTable) {
                tablesToDefrag.add(table.getName());
            }
        }
        
        for (Iterator t = tablesToDefrag.iterator(); t.hasNext();) {
            defragTable((String) t.next());
        }
    }
    
    @Override
    public int defragTable(String tableName) throws AxionException {
        try {
            AlterTableCommand alterTableCmd = new AlterTableCommand(tableName, false);
            return alterTableCmd.executeUpdate(this);
        } catch (AxionException e) {
            throw new AxionException("Unable to defrag table " + tableName, e);
        }
    }
    
    public File getDBDirectory() {
        return _dbDir;
    }
    
    @Override
    public TableFactory getTableFactory(String name) {
        if (null == name || "default".equals(name)) {
            return DEFAULT_TABLE_FACTORY;
        }
        return super.getTableFactory(name);
    }
    
    @Override
    public void migrate(int version) throws AxionException {
        if (version != -1 && version < 131) { // 0.3.1
            try {
                FileUtil.renameToUpperCase(getDBDirectory());
                _dbVersion = AXION_DB_VERSION;
                writeDbVersion();
            } catch (IOException e) {
                throw new AxionException(e);
            }
        }
    }
    
    @Override
    public void remount(File newdir) throws AxionException {
        _log.log(Level.FINE, "Remounting from " + _dbDir + " to " + newdir);
        _dbDir = newdir;
        super.remount(newdir);
    }
    
    @Override
    public void shutdown() throws AxionException {
        super.shutdown();
        releaseLockFile();
    }
    
    protected Table createSystemTable(String name) {
        return new MemoryTable(name, Table.SYSTEM_TABLE_TYPE);
    }
    
    @Override
    public void createDatabaseLink(DatabaseLink dblink) throws AxionException {
        try {
            persistDBLink(dblink);
        } catch (IOException ex) {
            throw new AxionException(ex);
        }
        super.createDatabaseLink(dblink);
    }
    
    @Override
    public void dropDatabaseLink(String name) throws AxionException {
        File dblink = new File(getDBDirectory().getAbsolutePath() + File.separator + "DBLINK", 
                name+".link");
        if(dblink.exists()) {
            dblink.delete();
        }
        super.dropDatabaseLink(name);
    }
    
    private void persistDBLink(DatabaseLink dblink) throws IOException {
        File links = new File(getDBDirectory().getAbsolutePath(), "DBLINK");
        if(!links.exists()) {
            links.mkdir();
        }
        File dblinkFile = new File(links.getAbsolutePath(), dblink.getName() + ".link");
        if(dblinkFile.exists()) {
            dblinkFile.delete();
        }
        dblinkFile.createNewFile();
        FileOutputStream out =  new FileOutputStream(dblinkFile);
        dblink.getProperties().store(out, "DBLink info");
    }
    
    private Properties loadProperties(File dbDir, Properties props) {
        if (null == props) {
            if (null != getBaseProperties()) {
                props = new Properties(getBaseProperties());
            } else {
                props = new Properties();
            }
            File propfile = new File(dbDir, "axiondb.properties");
            if (propfile.exists()) {
                _log.log(Level.FINE,"Loading properties from \"" + propfile + "\".");
                InputStream in = null;
                try {
                    in = new FileInputStream(propfile);
                    props.load(in);
                } catch (Exception e) {
                    // PROPOGATE UP!?!
                    _log.log(Level.SEVERE,"Exception while loading properties from \"" + propfile + "\".", e);
                } finally {
                    try {
                        in.close();
                    } catch (Exception e) {
                    }
                }
            }
        }
        return props;
    }
    
    private void obtainDBVersion() throws AxionException {
        File verFile = getDbFileName(".VER");
        if (!verFile.exists()) {
            verFile = new File(_dbDir, getName() + ".ver"); // old db
        }
        if (verFile.exists()) {
            DataInputStream in = null;
            try {
                in = new DataInputStream(new FileInputStream(verFile));
                _dbVersion = in.readInt();
            } catch (IOException e) {
                String msg = "Unable to read db version file";
                _log.log(Level.SEVERE,msg, e);
                throw new AxionException(msg);
            } finally {
                closeInputStream(in);
            }
        }
    }
    
    private void closeInputStream(DataInputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (Exception e) {
            }
        }
    }
    
    private void loadSequences() throws AxionException {
        File seqFile = getDbFileName(".SEQ");
        if (seqFile.exists()) {
            DataInputStream in = null;
            try {
                in = new DataInputStream(new FileInputStream(seqFile));
                int size = in.readInt();
                
                for (int i = 0; i < size; i++) {
                    Sequence seq;
                    if (_dbVersion > 102) {
                        seq = new Sequence();
                        seq.read(in);
                    } else {
                        String name = in.readUTF();
                        int value = in.readInt();
                        seq = new Sequence(name, value);
                    }
                    super.createSequence(seq);
                }
            } catch (Exception e) {
                String msg = "Unable to read sequence file";
                _log.log(Level.SEVERE,msg, e);
                throw new AxionException(msg, e);
            } finally {
                closeInputStream(in);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private void loadTables(File parentdir) throws AxionException {
        String[] tables = parentdir.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                File file = new File(dir, name);
                if (file.isDirectory()) {
                    File idx = new File(file, name + ".TYPE");
                    if (idx.exists()) {
                        return true;
                    }
                }
                return false;
            }
        });
        List views = new ArrayList();
        
        for (int i = 0; i < tables.length; i++) {
            _log.log(Level.FINE,"Recreating table " + tables[i]);
            File tabledir = new File(parentdir, tables[i]);
            File typefile = new File(tabledir, tables[i] + ".TYPE");
            String factoryname = null;
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(typefile)));
                factoryname = in.readUTF();
            } catch (IOException e) {
                throw new AxionException(e);
            } finally {
                try {
                    in.close();
                } catch (Exception e) {
                }
            }
            TableFactory factory = null;
            try {
                Class clazz = Class.forName(factoryname);
                factory = (TableFactory) (clazz.newInstance());
            } catch (Exception e) {
                throw new AxionException(e);
            }
            
            // load view after loading all tables
            if (factory instanceof TableViewFactory) {
                views.add(tabledir.getName());
            } else {
                Table table = factory.createTable(this, tabledir.getName());
                addTable(table);
            }
        }
        
        // loading views
        Iterator itr = views.iterator();
        TableViewFactory factory = new TableViewFactory();
        while (itr.hasNext()) {
            Table table = factory.createTable(this, (String) itr.next());
            addTable(table);
        }
    }
    
    private void obtainLockFile(Properties props) throws AxionException {
        String lockFileIgnoreProp = System.getProperty(IGNORE_LOCK_FILE_PROPERTY_NAME);
        if (lockFileIgnoreProp == null) {
            _ignoreLockFile = Boolean.valueOf(props.getProperty(DATABASE_LOCKFILE_IGNORE_PROPERTY_NAME)).booleanValue();
        } else {
            _ignoreLockFile = Boolean.valueOf(lockFileIgnoreProp).booleanValue();
        }
        if (isReadOnly()) {
            _ignoreLockFile = true;
        }
        if (!_ignoreLockFile) {
            File lock = new File(_dbDir, LOCK_FILE_NAME);
            if (lock.exists()) {
                throw new AxionException("The database directory at " + _dbDir.getAbsolutePath()
                + " appears to already be in use by another process. " + " If you feel you have reached this message in error, "
                        + "delete the file at " + lock.getAbsolutePath() + ".", 8002);
            }
            
            lock.deleteOnExit();
            FileWriter out = null;
            try {
                out = new FileWriter(lock);
                out.write("lock");
                out.write("\t");
                out.write(getName());
                out.write("\t");
                out.write(String.valueOf(System.currentTimeMillis()));
                out.flush();
            } catch (IOException e) {
                _log.log(Level.WARNING,"Unable to create lock file at " + lock.getAbsolutePath() + " due to exception.", e);
            } finally {
                try {
                    out.close();
                } catch (Exception e) {
                }
            }
        }
    }
    
    private void releaseLockFile() {
        if (!_ignoreLockFile) {
            File lock = new File(_dbDir, LOCK_FILE_NAME);
            if (lock.exists()) {
                if (!lock.delete()) {
                    _log.log(Level.WARNING,"Unable to delete lock file at " + lock.getAbsolutePath() + " due to exception.");
                }
            }
        }
    }
    
    private void loadDBLinks() throws AxionException {
        try {
            File dblinks = new File(getDBDirectory().getAbsolutePath(), "DBLINK");
            if(dblinks.exists()) {
                File[] files = dblinks.listFiles(new FileFilter(){
                    public boolean accept(File pathname) {
                        if(pathname.getName().endsWith(".link")) {
                            return true;
                        }
                        return false;
                    }
                });
                for(File file : files) {
                    Properties prop = new Properties();
                    prop.load(new FileInputStream(file));
                    DatabaseLink dblink = new DatabaseLink(
                            file.getName().substring(0, file.getName().indexOf(".")), prop);
                    createDatabaseLink(dblink);
                }
            }
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }
    
    //-------------------------------------------------------------- Attributes
    
    private static final TableFactory DEFAULT_TABLE_FACTORY = new DiskTableFactory();
    private static final String IGNORE_LOCK_FILE_PROPERTY_NAME = "org.axiondb.engine.DiskDatabase.IGNORE_LOCK_FILE";
    private static final String LOCK_FILE_NAME = "lockfile.txt";
    private File _dbDir = null;
    private boolean _ignoreLockFile = false;
    private static final int DB_MAJOR_VERSION = 0;
    private static final int DB_MINOR_VERSION = 3; // XXX CHANGE ME ON RELEASE XXX
    private static final int DB_INTERNAL_MINOR_VERSION = 1; // XXX RESET TO 0 ON RELEASE
    private static final int AXION_DB_VERSION = (100 * (DB_MAJOR_VERSION + 1)) + (10 * DB_MINOR_VERSION) + DB_INTERNAL_MINOR_VERSION;
    private int _dbVersion = -1;
    
    private static Logger _log = Logger.getLogger(DiskDatabase.class.getName());
    private static final String DATABASE_LOCKFILE_IGNORE_PROPERTY_NAME = "database.lockfile.ignore";
}
