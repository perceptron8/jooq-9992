package perceptron8;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.EnumSet;

import org.firebirdsql.management.FBManager;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Loader;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LoaderTest {
	private static final String fileName = "database.fdb";
	private static final String userName = "sysdba";
	private static final String password = "masterkey";

	@TempDir
	static Path tempDir;

	@BeforeAll
	public static void create() throws Exception {
		Files.setPosixFilePermissions(tempDir, EnumSet.allOf(PosixFilePermission.class));
		try (FBManager manager = new FBManager()) {
			manager.setFileName(tempDir.resolve(fileName).toString());
			manager.setUserName(userName);
			manager.setPassword(password);
			manager.setForceCreate(true);
			manager.setCreateOnStart(true);
			manager.start();
			manager.stop();
		}
	}

	@AfterAll
	public static void destroy() throws Exception {
		Files.delete(tempDir.resolve(fileName));
	}

	@Test
	public void execute() throws Exception {
		String url = "jdbc:firebirdsql://localhost/" + tempDir.resolve(fileName);
		try (
			Connection connection = DriverManager.getConnection(url, userName, password);
			DSLContext context = new DefaultDSLContext(connection, SQLDialect.FIREBIRD);
		) {
			context.execute("create table SOURCE (DATA varchar(50))");
			context.execute("create table TARGET (DATA varchar(50))");
			context.execute("insert into SOURCE (DATA) values ('a')");
			context.execute("insert into SOURCE (DATA) values ('bb')");
			context.execute("insert into SOURCE (DATA) values ('ccc')");
		}
		try (
			Connection connection1 = DriverManager.getConnection(url, userName, password);
			Connection connection2 = DriverManager.getConnection(url, userName, password);
			DSLContext context1 = new DefaultDSLContext(connection1, SQLDialect.FIREBIRD);
			DSLContext context2 = new DefaultDSLContext(connection2, SQLDialect.FIREBIRD);
		) {
			Table<?> source = context1.meta().getCatalog("").getSchema("").getTable("SOURCE");
			Table<?> target = context2.meta().getCatalog("").getSchema("").getTable("TARGET");
			try (Cursor<?> cursor = context1.select(DSL.asterisk()).from(source).orderBy(DSL.one().asc()).fetchLazy()) {
				Loader<?> loader = context2.loadInto(target).batchAll().loadRecords(cursor).fieldsFromSource().execute();
				assertTrue(loader.errors().isEmpty());
			}
		}
	}
}
