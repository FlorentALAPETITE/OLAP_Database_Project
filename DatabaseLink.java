import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseLink{

    private Connection connection;

    public DatabaseLink() throws Exception{
        System.out.println("-------- Oracle JDBC Connection Testing ------");
        
                try {
        
                    Class.forName("oracle.jdbc.driver.OracleDriver");
        
                } catch (ClassNotFoundException e) {
        
                    System.out.println("Driver not found");
                    e.printStackTrace();
                    return;
        
                }
        
                System.out.println("Oracle JDBC Driver Registered!");
        
                    
                try {
        
                    connection = DriverManager.getConnection(
                            "jdbc:oracle:thin:@oracle.ensinfo.sciences.univ-nantes.prive:1521:cienetdb", "admi2", "admi");
        
                } catch (SQLException e) {
        
                    System.out.println("Connection Failed! Check output console");
                    e.printStackTrace();
                    return;
        
                }
        
                if (connection != null) {
                    System.out.println("You made it, take control your database now!");
                } else {
                    throw new Exception("Failed to make connection!");
                }
        }


        public void readFromCSV(String filename){

        }
}