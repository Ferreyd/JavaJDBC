import com.sun.org.apache.xpath.internal.SourceTree;

import java.sql.*;

/**
 * Created by Nicolas on 03/10/2014.
 */

public class Main
{

    Connection con;
    String nomBD = "test";
    String url = "jdbc:mysql://flatbrains.eu/" + nomBD;
    String login = "NicoTPJDBC";
    String mdp = "1234";
    Statement stmt;

    public Main(String nomBD, String url, String login, String mdp) throws Exception
    {
        this.nomBD = nomBD;
        this.url = url;
        this.login = login;
        this.mdp = mdp;

        Class.forName("com.mysql.jdbc.Driver"); //Chargement du driver

        con = DriverManager.getConnection(url, login, mdp);
        checkForSQLWarnings(con.getWarnings());
        printInfo();
        stmt = con.createStatement();
    }

    public static void main(String[] args) throws Exception
    {

        String nomBD = "test";
        String url = "jdbc:mysql://flatbrains.eu/" + nomBD;
        String login = "NicoTPJDBC";
        String mdp = "1234";

        Main main = new Main(nomBD, url, login, mdp);
        main.creerTable();
        main.requetesSelectCafe();
        main.close();


    }

    /**
     * Print stdout some informations about the database connection
     */
    private void printInfo() throws Exception
    {
        // Get meta-data about the database
        DatabaseMetaData info = con.getMetaData();
        System.out.println("\nConnected to :\t" + info.getURL());
        System.out.println("Driver :\t" + info.getDriverName());
        System.out.println("Version :\t" + info.getDriverVersion());
    }

    /* Print stdout all pending SQLWarning warnings
    */
    private boolean checkForSQLWarnings(SQLWarning w) throws SQLException
    {
        boolean warning = false;
        if(w != null)
        {
            warning = true;
            System.out.println("\n**** Warning ****\n");

            while(w != null)
            {
                System.out.println("SQLState: " + w.getSQLState());
                System.out.println("Message:  " + w.getMessage());
                System.out.println("Vendor:   " + w.getErrorCode());
                System.out.println("");
                w = w.getNextWarning();
            }
        }
        return warning;
    }

    /**
     * Close the connection to the data base source
     */
    public void close() throws Exception
    {
        try
        {
            con.close();
            System.out.println("Test : Disconnecting ...");
        } catch(Exception e)
        {
            System.err.println("\n*** Exception caught in close()");
            throw e;
        }
    }

    /* Print stderr all pending SQLException exceptions
*/
    private void printSQLErrors(SQLException e)
    {
        while(e != null)
        {
            System.err.println("SQLState: " + e.getSQLState());
            System.err.println("Message:  " + e.getMessage());
            System.err.println("Vendor:   " + e.getErrorCode());
            System.err.println("");
            e = e.getNextException();
        }
    }

    /**
     * Méthode qui teste s'il y a des données dans une table
     *
     * @param table Nom de la table
     * @return vrai s'il y a une table, faux sinon
     * @throws SQLException
     */
    public boolean testExisteTables(String table) throws SQLException
    {
        DatabaseMetaData dbm = con.getMetaData();
        // check if "employee" table is there
        ResultSet tables = dbm.getTables(null, null, table, null);
        if(tables.next())
        {
            return true;
        } else
        {
            return false;
        }
    }

    /**
     * Test si les tables sont crées et si non, les crées avec les données
     * @throws SQLException
     */
    private void creerTable() throws SQLException
    {
        if(!testExisteTables("CAFE") && !testExisteTables("FOURNISSEUR"))
        {
            String creerTableCafe = "CREATE TABLE CAFE" +
                    "(NOM_CAFE VARCHAR(32)," +
                    " FO_ID INTEGER," +
                    " PRIX FLOAT," +
                    "VENTES INTEGER," +
                    " TOTAL INTEGER)";
            String creerTableFournsseur = "CREATE TABLE FOURNISSEUR" +
                    "(FO_ID INTEGER," +
                    "NOM_FO VARCHAR(32)," +
                    "RUE VARCHAR(32)," +
                    "VILLE VARCHAR(32)," +
                    "ETAT VARCHAR(32)," +
                    "CODE_POSTALE VARCHAR(32))";

            stmt.executeUpdate(creerTableCafe);
            stmt.executeUpdate(creerTableFournsseur);
            this.ajoutDonnees();
        }
    }

    private void ajoutDonnees() throws SQLException
    {
        String insertCafe = "INSERT INTO CAFE " +
                "VALUES ('Colombian', 101, 7.99, 0, 0), " +
                " ('French_Roast', 49, 8.99, 0, 0) , " +
                " ('Espresso', 150, 9.99, 0, 0) , " +
                " ('Colombian_Decaf', 101, 8.99, 0, 0) , " +
                " ('French_Roast_Decaf', 49, 9.99, 0, 0)";

        stmt.executeUpdate(insertCafe);
    }

    /**
     * SELECT * FROM CAFE
     * @return
     * @throws SQLException
     */
    public void requetesSelectCafe() throws SQLException
    {
        String res;
        ResultSet rs =  stmt.executeQuery("SELECT * FROM CAFE");
        System.out.println("NOM_CAFE\tFO_ID\tPRIX\tVENTES\tTOTAL");
        while(rs.next())
        {
            res = "";
            res += rs.getString("NOM_CAFE") + "\t";
            res += rs.getString("FO_ID") + "\t";
            res += rs.getString("PRIX") + "\t";
            res += rs.getString("VENTES") + "\t";
            res += rs.getString("TOTAL") + "\t";
            System.out.println(res);
        }
    }
}
