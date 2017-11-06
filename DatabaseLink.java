import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;


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
            int number;
            String sqlStatement;


            try {      
                //1   

                sqlStatement = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(DimAgency,dimAgency_pk) */ into DimAgency (agencyCode, agencyName, agencyType) VALUES (?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                number=1;
                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv
                
                while ((line = br.readLine()) != null) { 
                    number++;                 
                    String[] data = line.split(csvSeparator);
                    makeDimAgencyInsertStatement(data);
                    if(number%10000==0)
                        System.out.println(number);

                }
                statement.executeBatch();
                statement.close();

                //2

                sqlStatement = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(DimPlace,dimplace_pk) */ into DimPlace (city, state) VALUES (?,?)";

                statement = connection.prepareStatement(sqlStatement);
                number=1;
                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv
                
                while ((line = br.readLine()) != null) { 
                    number++;                 
                    String[] data = line.split(csvSeparator);
                    makeDimPlaceInsertStatement(data);
                    if(number%10000==0)
                        System.out.println(number);

                    
                }
                statement.executeBatch();
                statement.close();

                //3
                sqlStatement = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(DimDate,dimDate_pk) */ into DimDate (month, year) VALUES (?,?)";

                statement = connection.prepareStatement(sqlStatement);

                number=1;
                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv
                
                while ((line = br.readLine()) != null) { 
                    number++;                 
                    String[] data = line.split(csvSeparator);
                    makeDimDateInsertStatement(data);
                    if(number%10000==0)
                        System.out.println(number);

                    
                }
                statement.executeBatch();
                statement.close();

                //4
                sqlStatement = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(DimProfile,dimProfile_pk) */ into DimProfile (sex, age, race, ethnicity) VALUES (?,?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                number=1;
                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv
                
                while ((line = br.readLine()) != null) { 
                    number++;                 
                    String[] data = line.split(csvSeparator);
                    makeDimProfileInsertStatement(data);
                    if(number%10000==0)
                        System.out.println(number);

                    
                }
                statement.executeBatch();
                statement.close();

                //5
                sqlStatement = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(Fact,fact_pk) */ into Fact (idCrime, agencyCode, month, year, city, state, crimeType, crimeSolved, relationship, weapon, recordSource, victimCount, perpetratorCount, incident, victimSex, victimAge, victimRace, victimEthnicity, perpetratorSex, perpetratorAge, perpetratorRace, perpetratorEthnicity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                statement = connection.prepareStatement(sqlStatement);

                number=1;
                br = new BufferedReader(new FileReader(filename));
                line = br.readLine();  // ligne description csv
                
                while ((line = br.readLine()) != null) { 
                    number++;                 
                    String[] data = line.split(csvSeparator);
                    makeFactInsertStatement(data);
                    if(number%10000==0)
                        System.out.println(number);

                    
                }
                statement.executeBatch();
                statement.close();


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

        public void closeConnection(){
            try{
                connection.close();
            }
            catch(SQLException e){
                e.printStackTrace();
            }
        }

        private void makeDimAgencyInsertStatement(String [] data){   
            
            // PreparedStatement : insert DimAgency
            try{

                

                statement.setString(1,data[1]);
                statement.setString(2,data[2]);
                statement.setString(3,data[3]);
                statement.addBatch();
                             
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            // finally{
            //     try{
            //         statement.close();
            //     }
            //     catch(SQLException e){
            //         e.printStackTrace();
            //     }
            // }
        }


        private void makeDimPlaceInsertStatement(String [] data){   
          
            // PreparedStatement : insert DimPlace
            try{

            

                statement.setString(1,data[4]);
                statement.setString(2,data[5]);    

                statement.addBatch();              
            }
            catch(SQLIntegrityConstraintViolationException vc){
                   
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            // finally{
            //     try{
            //         statement.close();
            //     }
            //     catch(SQLException e){
            //         e.printStackTrace();
            //     }
            // }
        }

        private void makeDimDateInsertStatement(String [] data){   
             String sqlStatement;

            // PreparedStatement : insert DimDate
            try{
               
                statement.setString(1,data[7]);
                statement.setInt(2,Integer.parseInt(data[6]));    

                statement.addBatch();               
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            
            }
            // finally{
            //     try{
            //         statement.close();
            //     }
            //     catch(SQLException e){
            //         e.printStackTrace();
            //     }
            // }
        }

        private void makeDimProfileInsertStatement(String [] data){   
             String sqlStatement;

            // PreparedStatement : insert DimProfile victim
            try{
               
                statement.setString(1,data[11]); // victimSex
                statement.setInt(2,Integer.parseInt(data[12])); // victimAge
                statement.setString(3,data[13]); // victimRace
                statement.setString(4,data[14]); // victimEthnicity 

                statement.addBatch();           
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            // finally{
            //     try{
            //         statement.close();
            //     }
            //     catch(SQLException e){
            //         e.printStackTrace();
            //     }
            // }

            // PreparedStatement : insert DimProfile perpetrator
            try{
               

                statement.setString(1,data[15]); // perpetratorSex
                statement.setInt(2,Integer.parseInt(data[16])); // perpetratorAge
                statement.setString(3,data[17]); // perpetratorRace
                statement.setString(4,data[18]); // perpetratorEthnicity

                statement.addBatch();               
            }
            catch(SQLIntegrityConstraintViolationException vc){
                
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            // finally{
            //     try{
            //         statement.close();
            //     }
            //     catch(SQLException e){
            //         e.printStackTrace();
            //     }
            // }
        }

        private void makeFactInsertStatement(String [] data){   
             String sqlStatement;
            // PreparedStatement : insert Fact
            try{
                
                statement.setInt(1,Integer.parseInt(data[0])); // idCrime
                statement.setString(2,data[1]); // agencyCode
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
                statement.setInt(16,Integer.parseInt(data[12])); // victimAge
                statement.setString(17,data[13]); // victimRace
                statement.setString(18,data[14]); // victimEthnicity
                statement.setString(19,data[15]); // perpetratorSex
                statement.setInt(20,Integer.parseInt(data[16])); // perpetratorAge
                statement.setString(21,data[17]); // perpetratorRace
                statement.setString(22,data[18]); // perpetratorEthnicity


                statement.addBatch();                

            }
            catch(SQLException e){
                e.printStackTrace();
            }
            // finally{
            //     try{
            //         statement.close();
            //     }
            //     catch(SQLException e){
            //         e.printStackTrace();
            //     }
            // }


        }
}