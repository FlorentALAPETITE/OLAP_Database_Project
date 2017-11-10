import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Arrays;


public class DatabaseLink{

    private Connection connection;
    private PreparedStatement profileStatement;
    private PreparedStatement dateStatement;
    private PreparedStatement agencyStatement;
    private PreparedStatement factStatement;
    private PreparedStatement placeStatement;


    final String sqlAgencyStatement = "INSERT /*+ ignore_row_on_dupkey_index(DimAgency(agencyCode))*/ into DimAgency (agencyCode, agencyName, agencyType) VALUES (?,?,?)";
    final String sqlPlaceStatement = "INSERT /*+ ignore_row_on_dupkey_index(DimPlace,dimPlace_pk)*/ into DimPlace (city, state) VALUES (?,?)";
    final String sqlDateStatement = "INSERT /*+ ignore_row_on_dupkey_index(DimDate,dimDate_pk)*/ into DimDate (month, year, season) VALUES (?,?,?)";
    final String sqlProfileStatement = "INSERT /*+ ignore_row_on_dupkey_index(DimProfile,dimProfile_pk)*/ into DimProfile (sex, age, race, ethnicity) VALUES (?,?,?,?)";
    final String sqlFactStatement = "INSERT /*+ ignore_row_on_dupkey_index(Fact,fact_pk)*/ into Fact (idCrime, agencyCode, month, year, city, state, crimeType, crimeSolved, relationship, weapon, recordSource, victimCount, perpetratorCount, incident, victimSex, victimAge, victimRace, victimEthnicity, perpetratorSex, perpetratorAge, perpetratorRace, perpetratorEthnicity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private ArrayList<String> winterMonths;
    private ArrayList<String> summerMonths;
    private ArrayList<String> springMonths;
    private ArrayList<String> fallMonths;



    public DatabaseLink() throws Exception{
        System.out.println("-------- Oracle JDBC Connection ------");
        
                try {
        
                    Class.forName("oracle.jdbc.driver.OracleDriver");
        
                } catch (ClassNotFoundException e) {
        
                    System.out.println("Driver not found");
                    e.printStackTrace();
                    return;
        
                }
        
                System.out.println("Oracle JDBC Driver Registered");
        
                    
                try {
        
                    connection = DriverManager.getConnection(
                            "jdbc:oracle:thin:@oracle.ensinfo.sciences.univ-nantes.prive:1521:cienetdb", "admi2", "admi");
        
                } catch (SQLException e) {
        
                    System.out.println("Connection Failed! Check output console");
                    e.printStackTrace();
                    return;
        
                }
        
                if (connection != null) {
                    System.out.println("Oracle connection available");

                    winterMonths = new ArrayList<String>();
                    summerMonths = new ArrayList<String>();
                    springMonths = new ArrayList<String>();
                    fallMonths = new ArrayList<String>();

                    winterMonths.add("December");
                    winterMonths.add("January");
                    winterMonths.add("February");

                    summerMonths.add("June");
                    summerMonths.add("July");
                    summerMonths.add("August");

                    springMonths.add("March");
                    springMonths.add("April");
                    springMonths.add("May");

                    fallMonths.add("September");
                    fallMonths.add("October");
                    fallMonths.add("November");


                } else {
                    throw new Exception("Failed to make connection!");
                }

        }

        // Read the csv data file
        public void readFromCSV(String filename){
            BufferedReader br = null;
            String line;
            String csvSeparator = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
            int number;
            

            try {      
              
                
                int queryCpt = 1;

                
                // PreparedStatement creation

                instantiatePreparedStatement();

           
                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv
                String[] data;

                // for each line in the csv file
                while ((line = br.readLine()) != null) { 
                    data = line.split(csvSeparator);

                    queryCpt++;

                    if(queryCpt%500==0){                     
                        executeBatchStatement();
                        instantiatePreparedStatement();
                    }


                    makeDimAgencyInsertStatement(data);    
                    makeDimPlaceInsertStatement(data);    
                    makeDimDateInsertStatement(data);
                    makeDimProfileInsertStatement(data);  
                    makeFactInsertStatement(data);    
                }


                // Batch execution
                executeBatchStatement();
                
               

            } catch (FileNotFoundException e) {
                e.printStackTrace();

            } catch (IOException e) {
                e.printStackTrace();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
            finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }


        }

        // Close connection
        public void closeConnection(){
            try{
                connection.close();
                System.out.println("Connection closed");
            }
            catch(SQLException e){
                e.printStackTrace();
            }
        }

        private void makeDimAgencyInsertStatement(String [] data){   
            
            // PreparedStatement : insert DimAgency
            try{
              
                agencyStatement.setString(1,data[1]); 
                agencyStatement.setString(2,data[2]);
                agencyStatement.setString(3,data[3]);
                agencyStatement.addBatch();
                             
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            }           
        }


        private void makeDimPlaceInsertStatement(String [] data){   
          
            // PreparedStatement : insert DimPlace
            try{


                placeStatement.setString(1,data[4]);
                placeStatement.setString(2,data[5]);    

                placeStatement.addBatch();              
            }
            catch(SQLIntegrityConstraintViolationException vc){
                   
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            
        }

        private void makeDimDateInsertStatement(String [] data){   
             String sqlStatement;

            // PreparedStatement : insert DimDate
            try{
               
                dateStatement.setString(1,data[7]);
                dateStatement.setInt(2,Integer.parseInt(data[6]));    


                if(summerMonths.contains(data[7])){
                    dateStatement.setString(3,"summer");
                }
                else if(winterMonths.contains(data[7])){
                    dateStatement.setString(3,"winter");
                }
                else if(fallMonths.contains(data[7])){
                    dateStatement.setString(3,"fall");
                }
                else if(springMonths.contains(data[7])){
                    dateStatement.setString(3,"spring");
                }


                dateStatement.addBatch();               
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            
            }
            
        }

        private void makeDimProfileInsertStatement(String [] data){   
            String sqlStatement;

            // PreparedStatement : insert DimProfile victim
            try{
               
                profileStatement.setString(1,data[11]); // victimSex
                profileStatement.setInt(2,Integer.parseInt(data[12])); // victimAge
                profileStatement.setString(3,data[13]); // victimRace
                profileStatement.setString(4,data[14]); // victimEthnicity 

                profileStatement.addBatch();           
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            

            // PreparedStatement : insert DimProfile perpetrator
            try{
               

                profileStatement.setString(1,data[15]); // perpetratorSex
                profileStatement.setInt(2,Integer.parseInt(data[16])); // perpetratorAge
                profileStatement.setString(3,data[17]); // perpetratorRace
                profileStatement.setString(4,data[18]); // perpetratorEthnicity

                profileStatement.addBatch();               
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            
        }

        private void makeFactInsertStatement(String [] data){   
             String sqlStatement;
            // PreparedStatement : insert Fact
            try{
                
                factStatement.setInt(1,Integer.parseInt(data[0])); // idCrime
                factStatement.setString(2,data[1]); // agencyCode
                factStatement.setString(3,data[7]);  // month
                factStatement.setInt(4,Integer.parseInt(data[6])); // year
                factStatement.setString(5,data[4]);  // city
                factStatement.setString(6,data[5]);  // state
                factStatement.setString(7,data[9]);  // crimeType
                factStatement.setString(8,data[10]);  // crimeSolved
                factStatement.setString(9,data[19]);  // relationship
                factStatement.setString(10,data[20]);  // weapon
                factStatement.setString(11,data[23]); // recordSource
                factStatement.setInt(12,Integer.parseInt(data[21])); // victimCount
                factStatement.setInt(13,Integer.parseInt(data[22])); // perpetratorCount
                factStatement.setString(14,data[8]);   // incident
                factStatement.setString(15,data[11]); // victimSex
                factStatement.setInt(16,Integer.parseInt(data[12])); // victimAge
                factStatement.setString(17,data[13]); // victimRace
                factStatement.setString(18,data[14]); // victimEthnicity
                factStatement.setString(19,data[15]); // perpetratorSex
                factStatement.setInt(20,Integer.parseInt(data[16])); // perpetratorAge
                factStatement.setString(21,data[17]); // perpetratorRace
                factStatement.setString(22,data[18]); // perpetratorEthnicity


                factStatement.addBatch();                

            }
            catch(SQLException e){
                e.printStackTrace();
            }
          

        }


        private void instantiatePreparedStatement() throws SQLException{

            // SQL insert statement
            agencyStatement = connection.prepareStatement(sqlAgencyStatement);
            profileStatement = connection.prepareStatement(sqlProfileStatement);
            dateStatement = connection.prepareStatement(sqlDateStatement);
            factStatement = connection.prepareStatement(sqlFactStatement);
            placeStatement = connection.prepareStatement(sqlPlaceStatement);
        }


        private void executeBatchStatement() throws SQLException{
            agencyStatement.executeBatch();
            agencyStatement.close();


            placeStatement.executeBatch();
            placeStatement.close();

            dateStatement.executeBatch();
            dateStatement.close();

            profileStatement.executeBatch();
            profileStatement.close();

            factStatement.executeBatch();
            factStatement.close();

        }
}
