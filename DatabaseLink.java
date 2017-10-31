import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class DatabaseLink{

    private Connection connection;
    private PreparedStatement statement;

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
            BufferedReader br = null;
            String line = "";
            String csvSeparator = ",";

            try {

                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv

                int compteurCommit = 0;

                while ((line = br.readLine()) != null) {
                    ++compteurCommit;
                    String[] data = line.split(csvSeparator);
                    makeInsertStatement(data);

                    
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }


        }

        private void makeInsertStatement(String [] data){   
            String sqlStatement;

            // PreparedStatement : insert DimAgency
            try{

                sqlStatement = "INSERT into DimAgency (agencyCode, agencyName, agencyType) VALUES (?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setInt(1,Integer.parseInt(data[1]));
                statement.setString(2,data[2]);
                statement.setString(3,data[3]);

                statement.executeUpdate();
            }
            catch(SQLException e){
                e.printStackTrace();
            }


            // PreparedStatement : insert DimPlace
            try{

                sqlStatement = "INSERT into DimPlace (city, state) VALUES (?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setString(1,data[4]);
                statement.setString(2,data[5]);    

                statement.executeUpdate();
            }
            catch(SQLException e){
                e.printStackTrace();
            }

            // PreparedStatement : insert DimDate
            try{
                sqlStatement = "INSERT into DimDate (month, year) VALUES (?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setString(1,data[7]);
                statement.setInt(2,Integer.parseInt(data[6]));    

                statement.executeUpdate();
            }
            catch(SQLException e){
                e.printStackTrace();
            }


            // PreparedStatement : insert Fact
            try{
                sqlStatement = "INSERT into Fact (agencyCode, month, year, city, state, idVictim, idMurderer, crimeType, crimeSolved, relationship, weapon, recordSource, victimCount, murdererCount, incident) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setInt(1,Integer.parseInt(data[1]));
                statement.setString(2,data[7]); 
                statement.setInt(3,Integer.parseInt(data[6]));
                statement.setString(4,data[4]); 
                statement.setString(5,data[5]); 
                //statement.setString(6,(data[7]));  
                //statement.setString(7,(data[7])); 
                statement.setString(8,data[9]); 
                statement.setString(9,data[10]); 
                statement.setString(10,data[19]); 
                statement.setString(11,data[20]);
                statement.setString(12,data[23]); 
                statement.setString(13,data[21]); 
                statement.setString(14,data[22]);
                statement.setString(12,data[23]); 
                statement.setString(13,data[8]);   


                statement.executeUpdate();

            }
            catch(SQLException e){
                e.printStackTrace();
            }



        }
}