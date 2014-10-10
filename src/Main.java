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

    /**
     * Constructeur pour utiliser un autre serveur que flatbrains
     *
     * @param nomBD Nom de la base de données
     * @param url   URL vers la BD
     * @param login Login
     * @param mdp   Mot de passe
     * @throws Exception
     */
    public Main(String nomBD, String url, String login, String mdp) throws Exception
    {
        this.nomBD = nomBD; this.url = url + nomBD; this.login = login; this.mdp = mdp;

        connetion(url,nomBD,login,mdp);
    }


    /**
     * Constructeur pour se connecter à flatbrains par défaut
     *
     * @throws Exception
     */
    public Main() throws Exception
    {
        connetion(url,nomBD,login,mdp);
    }

    /**
     * Méthode qui gere la connection à la BD
     * @param nomBD Nom de la base de données
     * @param url   URL vers la BD
     * @param login Login
     * @param mdp   Mot de passe
     * @throws Exception
     */
    public void connetion(String url, String nomBD, String login, String mdp) throws Exception
    {
        Class.forName("com.mysql.jdbc.Driver"); //Chargement du driver

        con = DriverManager.getConnection(url, login, mdp); checkForSQLWarnings(con.getWarnings()); printInfo();
        stmt = con.createStatement();
    }

    /**
     * Print stdout some informations about the database connection
     */
    private void printInfo() throws Exception
    {
        // Get meta-data about the database
        DatabaseMetaData info = con.getMetaData(); System.out.println("\nConnected to :\t" + info.getURL());
        System.out.println("Driver :\t" + info.getDriverName());
        System.out.println("Version :\t" + info.getDriverVersion());
    }

    /* Print stdout all pending SQLWarning warnings
    */
    private boolean checkForSQLWarnings(SQLWarning w) throws SQLException
    {
        boolean warning = false; if(w != null)
    {
        warning = true; System.out.println("\n**** Warning ****\n");

        while(w != null)
        {
            System.out.println("SQLState: " + w.getSQLState()); System.out.println("Message:  " + w.getMessage());
            System.out.println("Vendor:   " + w.getErrorCode()); System.out.println(""); w = w.getNextWarning();
        }
    } return warning;
    }

    /**
     * Close the connection to the data base source
     */
    public void close() throws Exception
    {
        System.out.println("### Close ###"); try
    {
        con.close(); System.out.println("Test : Disconnecting ...");
    } catch(Exception e)
    {
        System.err.println("\n*** Exception caught in close()"); throw e;
    }
    }

    /* Print stderr all pending SQLException exceptions
*/
    private void printSQLErrors(SQLException e)
    {
        while(e != null)
        {
            System.err.println("SQLState: " + e.getSQLState()); System.err.println("Message:  " + e.getMessage());
            System.err.println("Vendor:   " + e.getErrorCode()); System.err.println(""); e = e.getNextException();
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
        ResultSet tables = dbm.getTables(null, null, table, null); if(tables.next())
    {
        return true;
    } else
    {
        return false;
    }
    }

    /**
     * Test si les tables sont crées et si non, les crées avec les données
     *
     * @throws SQLException
     */
    private void creerTable() throws SQLException
    {
        System.out.println("### Creer Table ###"); if(!testExisteTables("CAFE") && !testExisteTables("FOURNISSEUR"))
    {
        String creerTableCafe = "CREATE TABLE CAFE" +
                "(NOM_CAFE VARCHAR(32)," +
                " FO_ID INTEGER," +
                " PRIX FLOAT," +
                "VENTES INTEGER," +
                " TOTAL INTEGER)"; String creerTableFournsseur = "CREATE TABLE FOURNISSEURS" +
            "(FO_ID INTEGER," +
            "NOM_FO VARCHAR(40)," +
            "RUE VARCHAR(40)," +
            "VILLE VARCHAR(20)," +
            "ETAT VARCHAR(2)," +
            "CODE_POSTALE VARCHAR(5))";

        stmt.executeUpdate(creerTableCafe); stmt.executeUpdate(creerTableFournsseur); this.ajoutDonnees();
    }
    }

    private void ajoutDonnees() throws SQLException
    {
        System.out.println("### AJOUT DE DONNEES ###");
        String insertCafe = "INSERT INTO CAFE " +
                "VALUES ('Colombian', 101, 7.99, 0, 0), " +
                " ('French_Roast', 49, 8.99, 0, 0) , " +
                " ('Espresso', 150, 9.99, 0, 0) , " +
                " ('Colombian_Decaf', 101, 8.99, 0, 0) , " +
                " ('French_Roast_Decaf', 49, 9.99, 0, 0)";

        String insertFournisseur = "INSERT INTO FOURNISSEURS VALUES " +
                "(101,'Acme,Inc.',\"+ \"'99 Market Street', 'Groundsville', 'CA', '95199')," +
                "(49,'Superior " + "Coffee', '1 Party Place', 'Mendocino', 'CA','95460')," +
                "(150,'The High\"+ \"Ground', '100 Coffee Lane', 'Meadows', 'CA','93966')";

        stmt.executeUpdate(insertCafe); stmt.executeUpdate(insertFournisseur);
    }

    /**
     * SELECT * FROM CAFE
     *
     * @return
     * @throws SQLException
     */
    public void requetesSelectCafe() throws SQLException
    {
        System.out.println("### Requetes Select Cafe ###"); String res;
        ResultSet rs = stmt.executeQuery("SELECT * FROM CAFE");

        while(rs.next())
        {
            res = ""; res += rs.getString("NOM_CAFE") + "\n"; res += rs.getString("FO_ID") + "\t";
            res += rs.getString("PRIX") + "\t"; res += rs.getString("VENTES") + "\t";
            res += rs.getString("TOTAL") + "\t"; System.out.println(res);
        } System.out.println("__________________________");
    }

    public void requetesSelectFournisseur() throws SQLException
    {
        System.out.println("### Requetes Select Fournisseur ###"); String requete = "SELECT CAFE.NOM_CAFE " +
            "FROM CAFE, FOURNISSEURS " +
            "WHERE FOURNISSEURS.NOM_FO LIKE 'Acme,Inc.'" +
            " AND FOURNISSEURS.FO_ID = CAFE.FO_ID";

        ResultSet rs = stmt.executeQuery(requete); System.out.println("Les café acheté à Acme, Inc. : ");
        while(rs.next())
        {
            String nomCafe = rs.getString("NOM_CAFE"); System.out.println(" " + nomCafe);
        }
    }

    public void updateCafe() throws SQLException
    {
        System.out.println("### Update Cafe ###"); String update = ("UPDATE CAFE" +
            " SET VENTES = 75" +
            " WHERE NOM_CAFE LIKE 'Colombian'"); stmt.executeUpdate(update);
    }

    public void preparedStatements() throws SQLException
    {
        System.out.println("### Prepared Statements ###");

        PreparedStatement updateVentes; String updateString = "update CAFE set VENTES = ? WHERE NOM_CAFE LIKE ? ";
        updateVentes = con.prepareStatement(updateString); int[] VentesDeLaSemaine = {175, 150, 60, 155, 90};
        String[] cafes = {"Colombian", "French_Roast", "Espresso", "Colombian_Decaf", "French_Roast_Decaf"};
        int len = cafes.length; int paramSetInt = 1; int paramSetString = 2; for(int i = 0; i < len; i++)
    {
        updateVentes.setInt(paramSetInt, VentesDeLaSemaine[i]); updateVentes.setString(paramSetString, cafes[i]);
        updateVentes.executeUpdate();

    }
    }

    public static void main(String[] args) throws Exception
    {

        Main main = new Main();

        main.creerTable();

        main.requetesSelectCafe();

        main.updateCafe(); main.requetesSelectCafe();

        main.preparedStatements();

        main.requetesSelectCafe();

        main.requetesSelectFournisseur();

        main.close();
    }

}
