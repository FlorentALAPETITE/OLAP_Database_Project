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
                
                while ((line = br.readLine()) != null) {                  
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


            // PreparedStatement : insert DimProfile victim
            try{
                sqlStatement = "INSERT into DimProfile (sex, age, year, ethnicity) VALUES (?,?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setString(15,data[11]); // victimSex
                statement.setInt(16,data[12]); // victimAge
                statement.setString(17,data[13]); // victimRace
                statement.setString(18,data[14]); // victimEthnicity 

                statement.executeUpdate();
            }
            catch(SQLException e){
                e.printStackTrace();
            }

            // PreparedStatement : insert DimProfile perpetrator
            try{
                sqlStatement = "INSERT into DimProfile (sex, age, race, ethnicity) VALUES (?,?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setString(19,data[15]); // perpetratorSex
                statement.setInt(20,data[16]); // perpetratorAge
                statement.setString(21,data[17]); // perpetratorRace
                statement.setString(22,data[18]); // perpetratorEthnicity

                statement.executeUpdate();
            }
            catch(SQLException e){
                e.printStackTrace();
            }

            // PreparedStatement : insert Fact
            try{
                sqlStatement = "INSERT into Fact (idCrime, agencyCode, month, year, city, state, crimeType, crimeSolved, relationship, weapon, recordSource, victimCount, perpetratorCount, incident, victimSex, victimAge, victimRace, victimEthnicity, perpetratorSex, perpetratorAge, perpetratorRace, perpetratorEthnicity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                statement.setInt(1,Integer.parseInt(data[0])); // idCrime
                statement.setInt(2,Integer.parseInt(data[1])); // agencyCode
                statement.setString(3,data[7]);  // month
                statement.setInt(4,Integer.parseInt(data[6])); // year
                statement.setString(5,data[4]);  // city
                statement.setString(6,data[5]);  // state
                statement.setString(7,data[9]);  // crimeType
                statement.setString(8,data[10]);  // crimeSolved
                statement.setString(9,data[19]);  // relationship
                statement.setString(10,data[20]);  // weapon
                statement.setString(11,data[23]); // recordSource
                statement.setInt(12,Integer.parseInt(data[21])); // victimCount
                statement.setInt(13,Integer.parseInt(data[22])); // perpetratorCount
                statement.setString(14,data[8]);   // incident
                statement.setString(15,data[11]); // victimSex
                statement.setInt(16,data[12]); // victimAge
                statement.setString(17,data[13]); // victimRace
                statement.setString(18,data[14]); // victimEthnicity
                statement.setString(19,data[15]); // perpetratorSex
                statement.setInt(20,data[16]); // perpetratorAge
                statement.setString(21,data[17]); // perpetratorRace
                statement.setString(22,data[18]); // perpetratorEthnicity


                statement.executeUpdate();

            }
            catch(SQLException e){
                e.printStackTrace();
            }



        }
}