/**
 * 
 */
package database;

import java.util.ArrayList;

/**
 * @author Bastian Christoph
 *
 * This is a SQL helper class and basically designed in order
 * to simplify the SQL notation.
 * 
 * The fact should not be changed, that the SQL formulation is
 * kept as flat as possible with a low degree of abstraction
 * in order to develop effective methods for bottleneck-identification
 * and performance-improvements for database-related applications.
 * 
 * Using the padding character '_' in the names we want to ensure 
 * intuitive formatting- alignment in the notation.
 * 
 * Intuitively the SQL-clauses should be expandable over IDE-recommendation
 * 
 */
public class FlatSQL {
	
	ArrayList<String> transactionStmtList = new ArrayList<String>();
	
	/*
	 * specific: 
	 * - Join, Project
	 * - SELECT ... FROM ...
	 */
	public String SELECT_______;
	public String INTO_________;
	public String FROM_________;
	
	/*
	 * specific:
	 * - Used table, Table fields
	 * - INSERT INTO ... ( ... ) VALUES ...
	 * - UPDATE ... SET ... WHERE ...
	 */
	public String INSERT_INTO__;
	public String VALUES_______;
	public ArrayList<Long[]> VALUES_BATCH_ = new ArrayList<Long[]>();
	
	public String UPDATE_______;
	public String SET__________;
	
	/*
	 * specific:
	 * - TRUNCATE ...
	 * - DELETE FROM ... WHERE ...
	 */
	public String TRUNCATE_____;
	public String DELETE_______;
	
	/*
	 * - ALTER TABLE ...
	 * - LOCK TABLE ...
	 * - UNLOCK TABLE ...
	 */
	
	public String ALTER_TABLE__;
	public String LOCK_TABLES__;
	
	/*
	 * general: 
	 * - Select (Set-Field-Value, Set-Field-Ordering, Set-Count)
	 * - WHERE ...
	 * - GROUP BY ...
	 * - HAVING ...
	 */
	public String WHERE________;
	public String GROUP_BY_____;
	public String HAVING_______;
	
	public String ORDER_BY_____;
	public String LIMIT________;

	public FlatSQL() {
		this.clearTransactionList();
		this.clear();
	}
	
	public void clearTransactionList() {
		this.transactionStmtList.clear();
	}
	
	public void clear() {
		this.SELECT_______ = "";
		this.INTO_________ = "";
		this.FROM_________ = "";
		this.WHERE________ = "";
		
		this.GROUP_BY_____ = "";
		this.HAVING_______ = "";
		
		this.ORDER_BY_____ = "";
		this.LIMIT________ = "";
		
		this.INSERT_INTO__ = "";
		this.VALUES_______ = "";
		
		this.UPDATE_______ = "";
		this.SET__________ = "";
		
		this.TRUNCATE_____ = "";
		this.DELETE_______ = "";
		
		this.ALTER_TABLE__ = "";
		this.LOCK_TABLES__ = "";
	}
	
	public String filter_num(String aliasBase, String filterAlias, String filterField, int fieldValue) {
		
		// this.qSQL.UPDATE_______ += "JOIN (SELECT " + webRessourceID + " AS WebressourceID) AS Q1 ";
		// this.qSQL.UPDATE_______ += "ON (UTIP.WebRessourceID = Q1.WebRessourceID) ";
		// --> this.qSQL.UPDATE_______ += this.qSQL.filter_num("UTIP", "Q1", "WebressourceID", webRessourceID); 
		// --> replaces: WHERE WebressourceID = <WebressourceID>
		// --> moved to FROM / UPDATE clause
		
		// this.qSQL.UPDATE_______ += "JOIN (SELECT 1 AS leafPredicted) AS Q2 ";
		// this.qSQL.UPDATE_______ += "ON (UTIP.leafPredicted = Q2.leafPredicted) ";
		// --> this.qSQL.UPDATE_______ += this.qSQL.filter_num("UTIP", "Q2", "leafPredicted", 1);
		// --> replaces: WHERE leafPredicted = 1
		// --> moved to FROM / UPDATE clause
		
		String filterJoin = "";
		filterJoin += "JOIN (SELECT "+fieldValue+" AS "+filterField+") AS "+filterAlias+" ";
		filterJoin += "ON (";
		filterJoin += aliasBase+"."+filterField+" = "+filterAlias+"."+filterField;
		filterJoin += ") ";
		
		return filterJoin;
	}
	
	public String filter_str(String aliasBase, String filterAlias, String filterField, String fieldValue) {
		String filterJoin = "";
		filterJoin += "JOIN (SELECT '"+fieldValue+"' AS "+filterField+") AS "+filterAlias+" ";
		filterJoin += "ON (";
		filterJoin += aliasBase+"."+filterField+" = "+filterAlias+"."+filterField;
		filterJoin += ") ";
		
		return filterJoin;
	}
	
	public String toSqlString() {
		String sql = "";
		
		if(this.INSERT_INTO__.compareTo("") != 0) {
			sql += "INSERT INTO " + this.INSERT_INTO__ + " " + "\n";
			
			if(this.VALUES_______.compareTo("") != 0) {
				sql += "VALUES ";
				
				sql += "(";
				sql += this.VALUES_______;
				sql += ")";
				
			} else if(this.VALUES_BATCH_.size() > 1) {
				sql += "VALUES ";
				
				for(int i=0; i<this.VALUES_BATCH_.size()-1; i++) {
					Long[] dataSet = this.VALUES_BATCH_.get(i);
					sql += "(";
					if(dataSet.length > 1) {
						for(int j=0; j<dataSet.length-1; j++) {
							sql += dataSet[j] + ", ";
						}
						sql += dataSet[dataSet.length-1];
					} else {
						sql += dataSet[dataSet.length-1];
					}
					sql += "), ";
				}
				
				Long[] dataSet = this.VALUES_BATCH_.get( this.VALUES_BATCH_.size()-1 );
				sql += "(";
				if(dataSet.length > 1) {
					for(int j=0; j<dataSet.length-1; j++) {
						sql += dataSet[j] + ", ";
					}
					sql += dataSet[dataSet.length-1];
				} else {
					sql += dataSet[dataSet.length-1];
				}
				sql += ")";
				
			} else if(this.VALUES_BATCH_.size() == 1) {				
				sql += "VALUES ";
				
				Long[] dataSet = this.VALUES_BATCH_.get(0);
				sql += "(";
				if(dataSet.length > 1) {
					for(int j=0; j<dataSet.length-1; j++) {
						sql += dataSet[j] + ", ";
					}
					sql += dataSet[dataSet.length-1];
				} else {
					sql += dataSet[dataSet.length-1];
				}
				sql += ")";
			}
		}
		
		if(this.SELECT_______.compareTo("") != 0) {
			sql += "SELECT " + this.SELECT_______ + " " + "\n";
			
			/*
			 * SELECT t1.field1, t1.field2 
			 * INTO @variable1, @variable2
			 * ( FROM table t1 )
			 */
			if(this.INTO_________.compareTo("") != 0) {
				sql += "INTO " + this.INTO_________ + " " + "\n";
			}
			
			if(this.FROM_________.compareTo("") != 0) {
				sql += "FROM " + this.FROM_________ + " " + "\n";
				
//			} else {
//				System.out.println("Error: Wrong Sql Statement");
//				System.exit(1);
			}
			
			if(this.WHERE________.compareTo("") != 0) {
				sql += "WHERE " + this.WHERE________ + " " + "\n";
			}
			
			if(this.GROUP_BY_____.compareTo("") != 0) {
				sql += "GROUP BY " + this.GROUP_BY_____ + " " + "\n";
			}
			
			if(this.HAVING_______.compareTo("") != 0) {
				sql += "HAVING " + this.HAVING_______ + " " + "\n";
			}
			
			if(this.ORDER_BY_____.compareTo("") != 0) {
				sql += "ORDER BY " + this.ORDER_BY_____ + " " + "\n";
			}
			
			if(this.LIMIT________.compareTo("") != 0) {
				sql += "LIMIT " + this.LIMIT________ + " " + "\n";
			}
		}

		if(this.UPDATE_______.compareTo("") != 0) {
			sql += "UPDATE " + this.UPDATE_______ + " " + "\n";
			sql += "SET " + this.SET__________ + " " + "\n";
			
			if(this.WHERE________.compareTo("") != 0) {
				sql += "WHERE " + this.WHERE________ + " " + "\n";
			}
		}
		
		if(this.TRUNCATE_____.compareTo("") != 0) {
			sql += "TRUNCATE TABLE " + this.TRUNCATE_____ + " " + "\n";
		}
		
		if(this.ALTER_TABLE__.compareTo("") != 0) {
			sql += "ALTER TABLE " + this.ALTER_TABLE__ + " " + "\n";
		}
		
		if(this.LOCK_TABLES__.compareTo("") != 0) {
			sql += "LOCK TABLES " + this.LOCK_TABLES__ + " " + "\n";
		}
		
		if(this.DELETE_______.compareTo("") != 0) {
			sql += "DELETE " + this.DELETE_______ + " " + "\n";
			
			if(this.FROM_________.compareTo("") != 0) {
				sql += "FROM " + this.FROM_________ + " " + "\n";
			}
			if(this.WHERE________.compareTo("") != 0) {
				sql += "WHERE " + this.WHERE________ + " " + "\n";
			}
		}
		
		if( (sql.compareTo("") == 0) && (this.transactionStmtList.size() > 0) ) {
			for(int i=0; i<this.transactionStmtList.size(); i++) {
				sql += this.transactionStmtList.get(i);
			}
		}
		
		return sql;
	}

	public void addTransactionStmt() {
		String sql = this.toSqlString();
		
		this.transactionStmtList.add(sql + ";");
		
		this.clear();
	}
	
	public ArrayList<String> getTransactionStmtList() {
		return this.transactionStmtList;
	}
	
}
