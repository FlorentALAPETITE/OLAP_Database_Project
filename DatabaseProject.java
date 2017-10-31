class DatabaseProject{

    public static void main(String [] args){
        try{
            DatabaseLink dl = new DatabaseLink();
            dl.readFromCSV("csvTest.csv");

           
        }
        catch (Exception e){
            e.printStackTrace();
        }
    

    }
}