class DatabaseProject{

    public static void main(String [] args){
        try{
            DatabaseLink dl = new DatabaseLink();
            dl.readFromCSV("database.csv");
            dl.closeConnection();

           
        }
        catch (Exception e){
            e.printStackTrace();
        }
    

    }
}