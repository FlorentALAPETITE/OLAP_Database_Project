all:ExecuteDatabaseProject


CompileDatabaseProject: CreateDatabase  
	javac DatabaseProject.java


ExecuteDatabaseProject: CompileDatabaseProject  
	java -cp ojdbc8.jar:. DatabaseProject


CreateDatabase:
	sqlplus admi2@cienetdb/admi @create
