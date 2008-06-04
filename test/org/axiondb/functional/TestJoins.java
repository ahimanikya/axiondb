package org.axiondb.functional;

import java.sql.PreparedStatement;

import junit.framework.Test;
import junit.framework.TestSuite;
/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Girish Patil
 */
public class TestJoins extends AbstractFunctionalTest {
    public TestJoins(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestJoins.class);
    }

    public void testResolvingAllJoinTableColumns() throws Exception {
        try {
            _stmt.execute("create table t1 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s1 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s2 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s3 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s4 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s5 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s6 (emp_no numeric(10) not null, emp_name varchar(100))");

            _rset = _stmt.executeQuery("select s1.emp_name, s2.emp_name, s3.emp_name, s4.emp_name, s5.emp_name " +
                    " from t1 right outer join " +
                    " s1 left outer join s2 on s1.emp_no = s2.emp_no " +
                    " left outer join s3 on s1.emp_no = s3.emp_no " +
                    " left outer join s4 on s1.emp_no = s4.emp_no " +
                    " left outer join s5 on s1.emp_no = s5.emp_no " +
                    " on t1.emp_no = s1.emp_no ");

            assertNotNull(_rset);
        } catch (Exception ex){
            fail("No all columns of the joined tables are resolved. Ex:" + ex.getMessage());
        } finally{
            executeSiliently(_stmt, "drop table t1");
            executeSiliently(_stmt, "drop table s1");
            executeSiliently(_stmt, "drop table s2");
            executeSiliently(_stmt, "drop table s3");
            executeSiliently(_stmt, "drop table s4");
            executeSiliently(_stmt, "drop table s5");
            executeSiliently(_stmt, "drop table s6");
        }
    }

    public void testBindingVariablesInJoinCondition() throws Exception {
        PreparedStatement ps = null;
        try {
            _stmt.execute("create table t1 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s1 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s2 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s3 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s4 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s5 (emp_no numeric(10) not null, emp_name varchar(100))");
            _stmt.execute("create table s6 (emp_no numeric(10) not null, emp_name varchar(100))");

            _stmt.execute("insert into t1(emp_no , emp_name) values (1, 'John Lenon') ");
            _stmt.execute("insert into s1(emp_no , emp_name) values (1, 'John Murphy') ");
            _stmt.execute("insert into s2(emp_no , emp_name) values (1, 'Johnny Carson') ");
            _stmt.execute("insert into s3(emp_no , emp_name) values (1, 'Johnny Lefante') ");
            _stmt.execute("insert into s3(emp_no , emp_name) values (1, 'John Wright') ");
            _stmt.execute("insert into s4(emp_no , emp_name) values (1, 'John Morgan') ");
            _stmt.execute("insert into s5(emp_no , emp_name) values (1, 'John Potter') ");

            ps = _conn.prepareStatement("select s1.emp_name, s2.emp_name, s3.emp_name, s4.emp_name, s5.emp_name " +
                    " from t1 inner join " +
                    " s1 inner join s2 on s1.emp_no = s2.emp_no " +
                    " inner join s3 on (s1.emp_no = s3.emp_no) AND (s3.emp_name = ?) " +
                    " inner join s4 on s1.emp_no = s4.emp_no " +
                    " inner join s5 on s1.emp_no = s5.emp_no " +
                    " on t1.emp_no = s1.emp_no ");
            ps.setString(1, "John Wright");
            _rset = ps.executeQuery();
            assertNotNull(_rset);
            assertNRows(1, _rset);
        } catch (Exception ex){
            fail("No all columns of the joined tables are resolved. Ex:" + ex.getMessage());
        } finally{
            closeDBResources(null, ps, _rset);
            executeSiliently(_stmt, "drop table t1");
            executeSiliently(_stmt, "drop table s1");
            executeSiliently(_stmt, "drop table s2");
            executeSiliently(_stmt, "drop table s3");
            executeSiliently(_stmt, "drop table s4");
            executeSiliently(_stmt, "drop table s5");
            executeSiliently(_stmt, "drop table s6");
        }
    }

}
