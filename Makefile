all:ExecuteDatabaseProject


CompileDatabaseProject: CreateDatabase UnzipDatabase 
	javac DatabaseProject.java


ExecuteDatabaseProject: CompileDatabaseProject  
	java -cp ojdbc8.jar:. DatabaseProject


UnzipDatabase:
	unzip database.csv.zip


CreateDatabase:
	sqlplus admi2@cienetdb/admi @create
