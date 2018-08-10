import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main( String args[] )
    {

        final Integer[] angles = {0,45,90,135,180,225,270,315};
        final String dbPath = "";

        try {
            Class.forName("org.sqlite.JDBC");
            Connection connection = null;
            connection = DriverManager.getConnection("jdbc:sqlite:"+dbPath);
            Statement statement = null;
            statement = connection.createStatement();
            ResultSet user = statement.executeQuery("select distinct uid from user");
            List<Integer> uids = new LinkedList<>();

            while (user.next()) {
                if (user.getInt("uid")!=2014051957)
                    uids.add(user.getInt("uid"));
            }

            for (final Integer uid: uids) {
                new Thread() {
                    public void run() {
                        try {
                            Map<Integer,String> actions = new HashMap<>();
                            actions.put(0,"Still");
                            actions.put(1,"Running");
                            actions.put(2,"Jumping Upward");
                            actions.put(3,"Upstairs");
                            actions.put(4,"Downstairs");
                            actions.put(5,"Cycling");
                            actions.put(6,"Walking50m");
                            Connection c = null;
                            c = DriverManager.getConnection("jdbc:sqlite:"+dbPath);
                            Statement stmt = null;
                            stmt = c.createStatement();
                            for (Integer angle: angles) {
                                for (Map.Entry<Integer,String> action: actions.entrySet()) {
                                    String filename = uid+"-"+action.getValue()+"-"+angle+".txt";
                                    FileWriter writer = new FileWriter(filename);
                                    System.out.println("outputting to "+filename);
                                    ResultSet resultSet = stmt.executeQuery("select acc_x,acc_y,acc_z from acts join datarow on acts.group_id = datarow.group_id  where acts.group_id in (select max(acts.group_id) from acts join datarow on acts.group_id = datarow.group_id group by action_id,uid,angle) and uid = " +uid+" and angle = "+ angle +" and action_id = "+ action.getKey());
                                    while (resultSet.next()){
                                        writer.write(resultSet.getDouble("acc_x")+" ");
                                        writer.write(resultSet.getDouble("acc_x")+" ");
                                        writer.write(resultSet.getDouble("acc_y")+"\r\n");
                                    }
                                    System.out.println("output "+filename+" finish.");
                                    writer.close();
                                }

                            }
                        } catch ( Exception e ) {
                            System.err.println(e);
                            System.exit(0);
                        }

                    }
                }.start();

            }




        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        System.out.println("Opened database successfully");
    }
}
