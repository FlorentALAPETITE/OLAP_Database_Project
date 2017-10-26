all:ExecuteDatabaseCreation


ExecuteDatabaseCreation: CompileDatabaseCreation
	javac DatabaseCreation.java


CompileDatabaseCreation: UnzipDatabase
	java -cp ojdbc8.jar:. DatabaseCreation


UnzipDatabase:
	unzip database.csv.zip
