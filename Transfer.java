// Transfer money using serializable transactions
// with retry logic for deadlocks and/or Oracle serialization errors
// for Oracle, run with   
//     java -classpath ojdbc6.jar;. Transfer         on Windows
//                     (use : instead of ; on UNIX
// Note: change the variable oracleServer to "localhost" for use with a tunnel.
// Note: When we have multiple transactions in a program, we should have each
// transaction in its own method, here doTransfer, with the rollbacks and commits as needed
// so that at the method return, there is no transaction running.
import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

class Transfer {
	public static final String oracleServer = "dbs3.cs.umb.edu";
	public static final String oracleServerSid = "dbs3";
	public static final int N_TX_RETRIES = 4;
	public static final int TX_RETRY_WAIT_MS = 1000;
	public static final String DEADLOCK_SQLSTATE = "61000"; // for Oracle 
	// for Oracle Snapshot Isolation errors:
	public static final String SERIALIZATION_ERROR_SQLSTATE = "72000";

	public static void main(String args[]) {
		Scanner in;
		Connection conn = null;
		try {
			in = new Scanner(System.in);
			conn = getConnected(in);
			conn.setAutoCommit(false); // take over transaction lifetime
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

			createAccountsTable(conn);
			System.out.println("Table created.");
			showAccountsTable(conn);

			doCustomerTransfers(conn, in);
			conn.close(); // actually, will be closed by exit in any case
			System.out.println("Finished, exiting");
		} catch (SQLException e) {
			System.out.println("Failed because of database problem: " + e);
			printSQLException(e);
			System.exit(1); // this closes the Connection
		} catch (InterruptedException e) {
			System.out.println("Exiting because of user interrupt");
			System.exit(2); // user said to quit
		} catch (Exception e) {
			System.out.println("Failed because of error: " + e);
			System.exit(3); // this closes the Connection
		}
	}

	// do loop of transfers, until the user inputs a nullstring from-account
	// throws SQLException on database error, InterruptedException on 
	// user interrupt, IOException if problem in input from user
	public static void doCustomerTransfers(Connection conn, Scanner in)
			throws Exception {
		String fromAccount, toAccount;
		double transferDollars;
		while ((fromAccount = readEntry(in, "from account no.: ")).length() > 0) {
			toAccount = readEntry(in, "to account no.: ");
			transferDollars = Double.parseDouble(readEntry(in, "amount: "));
			System.out.println("Doing transfer of " + transferDollars
					+ " from " + fromAccount + " to " + toAccount);
			doTransfer(conn, fromAccount, toAccount, transferDollars);
			System.out.println("Transfer complete");
			showAccountsTable(conn);
		}
	}

	// one or a deadlock-related sequence of tries to do a transfer
	// transaction. May fail with:
	// --SQLException: DB problem (with rollback done/attempted)
	// --InterruptedException: User interrupted the sleep between tries
	// (so no transaction is in process if this happens either)
	// --other exception (with rollback done/attempted)
	public static void doTransfer(Connection conn, String fromAccount,
			String toAccount, double dollars) throws Exception {
		for (int i = 0; i < N_TX_RETRIES + 1; i++) {
			try {
				transfer(conn, fromAccount, toAccount, dollars);
				conn.commit();
				return; // success
			} catch (SQLException e) {
				rollbackAfterException(conn);
				if (!e.getSQLState().equals(DEADLOCK_SQLSTATE)
						&& !e.getSQLState().equals(SERIALIZATION_ERROR_SQLSTATE)
						|| i == N_TX_RETRIES)
					throw e; // non-deadlock error or out of retries
				// Here with deadlock or Oracle serialization error--
				Thread.sleep(TX_RETRY_WAIT_MS); // delay before retry
			} catch (Exception e) {
				rollbackAfterException(conn);
				throw e; // non-SQLException
			}
		}
	}
	
	// The caller should issue its own exception based on the
	// original exception (or do retry)
	public static void rollbackAfterException(Connection conn) {
		try {
			conn.rollback();
		} catch (Exception e) {
			// discard secondary exception--probably server can't be reached
		}
	}
	
	// one transfer operation, to be tried and retried on deadlock
	public static void transfer(Connection conn, String fromAccount,
			String toAccount, double dollars) throws Exception {
		Statement stmt = conn.createStatement();
		try {
			stmt.executeUpdate("update accounts set balance = balance - "
					+ dollars + " where account_no = '" + fromAccount + "'");
			stmt.executeUpdate("update accounts set balance = balance + "
					+ dollars + " where account_no = '" + toAccount + "'");
		} 
		  finally {
			stmt.close();
		}
	}

	public static void createAccountsTable(Connection conn) throws Exception {
		Statement stmt = conn.createStatement();
		try {
			try {
				stmt.execute("drop table accounts");
			} catch (SQLException e) {
				// assume not there yet, so OK to continue
				conn.rollback();
			}
			stmt
					.execute("create table accounts(account_no int, balance float)");
			for (int i = 1; i < 10; i++)
				stmt.execute("insert into accounts values (" + i + ", " + 100.0
						* i + ")");
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
			} // discard secondary exception
			throw e; // rethrow original exception
		} finally {
			// Close the statement, no matter what happens above
			stmt.close(); // this also closes the ResultSet, if any
		}
	}

	public static void showAccountsTable(Connection conn) throws Exception {
		Statement stmt = conn.createStatement();
		try {
			System.out.println("Accounts table: \n account_no   balance");
			ResultSet rset = stmt.executeQuery("select * from accounts");
			while (rset.next())
				System.out.println(rset.getInt("account_no") + ",  "
						+ rset.getDouble("balance"));
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
			} // discard secondary exception
			throw e; // rethrow original exception
		} finally {
			stmt.close();
		}
	}

	public static Connection getConnected(Scanner in) throws Exception {
		String driver = "oracle.jdbc.OracleDriver";
		Class.forName(driver); // load driver: it runs a static
		// initializer that loads other classes, etc.

		// Prompt the user for connect information
		String user = readEntry(in, "user: ");
		String password = readEntry(in, "password: ");
		String connStr = "jdbc:oracle:thin:@" + oracleServer + ":1521:"
				+ oracleServerSid;

		System.out.println("using connection string: " + connStr);
		System.out.print("Connecting to the database...");
		System.out.flush();

		// Connect to the database
		Connection conn = DriverManager.getConnection(connStr, user, password);
		System.out.println("connected.");
		return conn;
	}
	
	// print out all exceptions connected to e by nextException or getCause
	static void printSQLException(SQLException e) {
		// SQLExceptions can be delivered in lists (e.getNextException)
		// Each such exception can have a cause (e.getCause, from Throwable)
		while (e != null) {
			System.out.println("SQLException Message:" + e.getMessage());
			Throwable t = e.getCause();
			while (t != null) {
				System.out.println("SQLException Cause:" + t);
				t = t.getCause();
			}
			e = e.getNextException();
		}
	}

	// super-simple prompted input from user
	public static String readEntry(Scanner in, String prompt)
			throws IOException {
		System.out.print(prompt);
		return in.nextLine().trim();
	}
}
