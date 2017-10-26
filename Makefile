all:ExecuteDatabaseProject


ExecuteDatabaseProject: CompileDatabaseProject CreateDatabase
	javac DatabaseProject.java


CompileDatabaseProject: UnzipDatabase 
	java -cp ojdbc8.jar:. DatabaseProject


UnzipDatabase:
	unzip database.csv.zip


CreateDatabase:
	sqlplus admi2@cienetdb/admi @create
