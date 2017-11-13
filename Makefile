all:ExecuteDatabaseProject


CompileDatabaseProject: CreateDatabase CreateBin
	javac -d bin src/*


ExecuteDatabaseProject: CompileDatabaseProject  
	java -cp "./bin:./ojdbc8.jar" DatabaseProject 


CreateDatabase:
	sqlplus admi2@cienetdb/admi @create


CreateBin:
	mkdir -p bin
