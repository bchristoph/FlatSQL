/**
 * 
 */
package database;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

/**
 * @author Bastian Christoph
 *
 */
@SuppressWarnings("unchecked")
public class DatabaseTools {
	
	private java.sql.Connection conn;
	
	private boolean showDebug;
	
	private String host;
	private String user;
	private String passwd;
	private String dbName;
	
	private Statement stmt;
	private PreparedStatement stmtPrepared;
	private ResultSet currentRS;
	
	private ArrayList<String> rowValuesString;
	private ArrayList<Integer> rowValuesInteger;
	private ArrayList<Double> rowValuesDouble;
	private ArrayList<ArrayList<String>> rowList;
	
	private int batchSize = 1;
	private int batchItems = 0;
	
	private long timeStart;
	private long timeEnd;
	private long timeDiff;
	private int counter = 0;
	
	private long dbOpTimeTotal;
	private long dbOpTotalItems_INS_UPD;
	private long dbOpTotalItems_SEL;
	
	private int countCot = 10;
	private int countSequence = 1000;
	
	public DatabaseTools() {
		this.registerDriver();
		
		this.host = "localhost";
//		this.host = "188.40.201.159";
//		this.host = "46.38.233.251";
//		this.host = "46.38.250.53";
		this.user = "iKnowLibsql1";
		this.passwd = "cnv1k7j6";
		this.dbName = "iKnowLibsql1";
		
		this.clear();
	}
	
	public void clear() {
		this.stmt = null;
		this.stmtPrepared = null;
		this.currentRS = null;
		
		this.rowValuesString = new ArrayList<String>();
		this.rowValuesInteger = new ArrayList<Integer>();
		this.rowValuesDouble = new ArrayList<Double>();
		
		this.rowList = new ArrayList<ArrayList<String>>();
		
		this.counter = 0;
		this.timeStart = System.currentTimeMillis();
		
		this.dbOpTimeTotal = 0;
		this.dbOpTotalItems_INS_UPD = 0;
		this.dbOpTotalItems_SEL = 0;
	}
	
	public void runOnServer(boolean useLocalHost) {
		if(useLocalHost) {
			this.host = "localhost";
			
			countCot = 100;
			countSequence = 10000;
		}
	}
	
	public void useReutersRcv1() {
		this.user = "iKnowLibsql2";
		this.passwd = "cnv1k7j6";
		this.dbName = "iKnowLibsql2";
	}

	private void registerDriver() {
		try {
			Class.forName( "com.mysql.jdbc.Driver" ).newInstance();
			
		} catch (InstantiationException e) {
			e.printStackTrace();
			
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void setShowDebug(boolean showDebug) {
		this.showDebug = showDebug;
	}

	public long getDbTimeTotal() {
		return this.dbOpTimeTotal;
	}
	
	public long getDbTotalItems_INS_UPD() {
		return this.dbOpTotalItems_INS_UPD;
	}
	
	public long getDbTotalItems_SEL() {
		return this.dbOpTotalItems_SEL;
	}
	
	public void createConnection() {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		try {
			this.conn = DriverManager.getConnection( 
							"jdbc:mysql://" + this.host + ":3306/" + 
							this.dbName + "?useUnicode=true&characterEncoding=UTF-8", 
							this.user, this.passwd );
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
	}
	
	public void executeStmtInsertUpdate(String sql) {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		int rowsEffected = 0;
		
		try {
			if(this.stmt == null) {
				this.stmt = this.conn.createStatement();
			}
			
			if(this.showDebug) {
				System.out.println("executing: " + sql);
			}
			
			// System.out.println(sql);
			
			rowsEffected = this.stmt.executeUpdate(sql);
			if(rowsEffected == 0) rowsEffected++;
			
			
			counter++;
			if(counter % countCot == 0) {
				System.out.print(".");
			}
			
			/*
			 * one block: <countSequence> lines
			 */
			if(counter % countSequence == 0) {
				timeEnd = System.currentTimeMillis();
				timeDiff = timeEnd - timeStart;
				
				System.out.print(" " + countSequence + " statements done;");
				System.out.print(" time elapsed: " + timeDiff + " ms");
				
				System.out.println();
				
				timeStart = System.currentTimeMillis();
			}
			
		} catch(MySQLIntegrityConstraintViolationException e) {
//			if(this.showDebug) {
				System.out.println();
				System.out.println("msg: "+e.getMessage());
				System.out.println(sql);
				System.out.println("Entry currently exists: skipping");
				
				System.out.println();
				System.out.println( sql );
				System.exit(1);
//			}
			
		} catch (SQLException e) {
			System.out.println( sql );
			
			e.printStackTrace();
			
			System.exit(1);
		}
		
//		this.dbOpTotalItems_INS_UPD += rowsEffected;
		this.dbOpTotalItems_INS_UPD += 1;
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
	}
	
	public void executeStmtSelect(String sql) {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		try {
			if(this.stmt == null) {
				this.stmt = this.conn.createStatement();
			}
			
			if(this.showDebug) {
				System.out.println("executing: " + sql);
			}
			
			this.currentRS = this.stmt.executeQuery(sql);
			
		} catch(MySQLIntegrityConstraintViolationException e) {
			System.out.println("Entry currently exists: skipping");
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
	}
	
	public void executeTransactionBEGIN() throws SQLException {
		try {
			this.conn.setAutoCommit(false);
			this.stmt = this.conn.createStatement();
			
		} catch(MySQLIntegrityConstraintViolationException e) {
			throw e;
			
		} catch (SQLException e) {
			throw e;
		}
	}
	
	public void executeTransactionCOMMIT() throws SQLException {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		try {
			this.conn.commit();
			this.conn.setAutoCommit(true);
			this.stmt = null;
			
		} catch(MySQLIntegrityConstraintViolationException e) {
			throw e;
			
		} catch (SQLException e) {
			throw e;
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
	}

	public void executeTransaction(FlatSQL qSQL) {
		try {
			this.executeTransactionBEGIN();
			
			ArrayList<String> stmtList = qSQL.getTransactionStmtList();
			
			for(int i=0; i<stmtList.size(); i++) {
				String sql = stmtList.get(i);
				
				if(sql.startsWith("SELECT")) {
					this.executeStmtSelect(sql);
					
				} else if(sql.startsWith("INSERT")) {
					this.executeStmtInsertUpdate(sql);
					
				} else if(sql.startsWith("UPDATE")) {
					this.executeStmtInsertUpdate(sql);
				}
			}
			
			this.executeTransactionCOMMIT();
			
			qSQL.clearTransactionList();
			
		} catch(MySQLIntegrityConstraintViolationException e) {
			System.out.println("Entry currently exists: skipping");
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	public void clearTableDELETE(String tableName) {
		
		String sql = "";
		sql += "DELETE FROM ";
		sql += tableName;
		
		this.executeStmtInsertUpdate(sql);
		
	}
	
	public void clearTableTRUNCATE(String tableName) {
		
		String sql = "";
		sql += "TRUNCATE TABLE ";
		sql += tableName;
		
		this.executeStmtInsertUpdate(sql);
		
	}
	
	public void unlockTables() {
		
		String sql = "";
		sql += "UNLOCK TABLES ";
		
		this.executeStmtInsertUpdate(sql);
		
	}
	
	public void createPreparedStmt(String sql) {
		try {
			if(this.stmtPrepared == null) {
				this.stmtPrepared = this.conn.prepareStatement(sql);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	public void setParamString(int index, String param) {
		try {
			this.stmtPrepared.setString(index, param);
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	public void executePreparedStmt() {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		try {
			this.stmtPrepared.addBatch();
			this.batchItems++;
			
			if(this.batchItems == this.batchSize) {
//				this.stmtPrepared.executeUpdate();
				this.stmtPrepared.executeBatch();
				this.stmtPrepared.clearBatch();
				this.batchItems = 0;				
			}
			
			this.stmtPrepared.clearParameters();
			this.stmtPrepared.clearWarnings();
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
	}
	
	public String getFieldNumAsString(int fieldNum) {
		String fieldValue = "";
		
		try {
			if(this.currentRS.next()) {
				fieldValue = this.currentRS.getString(fieldNum);
				
				this.dbOpTotalItems_SEL++;
				
				return fieldValue;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		return null;
	}
	
	public ArrayList<String> getFirstResultRow() {
		this.rowValuesString = (ArrayList<String>) this.rowValuesString.clone();
		this.rowValuesString.clear();
		
		try {
			if(this.currentRS.next()) {
				int columnCount = this.currentRS.getMetaData().getColumnCount();
				
				for(int i=1; i<= columnCount; i++) {
					String fieldValue = this.currentRS.getString(i);
					
//					System.out.println(fieldValue);
					
					this.rowValuesString.add(fieldValue);
				}
				
				this.dbOpTotalItems_SEL++;
			}
			
			return this.rowValuesString;
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		return null;
	}
	
	public ArrayList<ArrayList<String>> getResultRowList() {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		this.rowList.clear();
		
		try {
			while(this.currentRS.next()) {
				this.rowValuesString = (ArrayList<String>) this.rowValuesString.clone();
				this.rowValuesString.clear();
				
				int columnCount = this.currentRS.getMetaData().getColumnCount();
				
				for(int i=1; i<= columnCount; i++) {
					String fieldValue = this.currentRS.getString(i);
					
//					System.out.println(fieldValue);
					
					this.rowValuesString.add(fieldValue);
				}
				
				this.rowList.add(this.rowValuesString);
				
				this.dbOpTotalItems_SEL++;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
		
		return this.rowList;
	}
	
	public ArrayList<String> getFirstColumn() {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		this.rowValuesString = (ArrayList<String>) this.rowValuesString.clone();
		this.rowValuesString.clear();
		
		try {
			while(this.currentRS.next()) {
				String fieldValue = this.currentRS.getString(1);
				this.rowValuesString.add(fieldValue);
				
				this.dbOpTotalItems_SEL++;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
		
		return this.rowValuesString;
	}
	
	public ArrayList<Integer> getFirstColumnAsInteger() {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		this.rowValuesInteger = (ArrayList<Integer>) this.rowValuesInteger.clone();
		this.rowValuesInteger.clear();
		
		try {
			while(this.currentRS.next()) {
				Integer fieldValue = Integer.parseInt(this.currentRS.getString(1));
				this.rowValuesInteger.add(fieldValue);
				
				this.dbOpTotalItems_SEL++;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
		
		return this.rowValuesInteger;
	}
	
	public ArrayList<Double> getFirstColumnAsDouble() {
		
		Date opStart;
		Date opEnd;
		
		opStart = new Date();
		
		
		
		this.rowValuesDouble = (ArrayList<Double>) this.rowValuesDouble.clone();
		this.rowValuesDouble.clear();
		
		try {
			while(this.currentRS.next()) {
				Double fieldValue = Double.parseDouble( this.currentRS.getString(1) );
				this.rowValuesDouble.add(fieldValue);
				
				this.dbOpTotalItems_SEL++;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			
			System.exit(1);
		}
		
		
		
		opEnd = new Date();
		long opTime = opEnd.getTime() - opStart.getTime();
		
		this.dbOpTimeTotal += opTime;
		
		
		
		return this.rowValuesDouble;
	}
	
}
