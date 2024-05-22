package idb;


import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import idb.core.Database;
import idb.core.SQLQueryHandler;
import idb.resource.JsonResource;
import idb.utils.JsonDatabaseHandler;
import io.muserver.MuServer;
import io.muserver.MuServerBuilder;
import io.muserver.rest.RestHandlerBuilder;
public class MuMain {

    public static void main(String[] args) {
        try {
            JacksonJsonProvider jacksonJsonProvider = new JacksonJsonProvider();

            // Initialize Database and JsonDatabaseHandler
            Database database = new Database();
            database.loadTablesFromConfig("application.properties");
            JsonDatabaseHandler jsonDatabaseHandler = new JsonDatabaseHandler(database, "src/main/resources/json_structure.yaml");
            SQLQueryHandler sqlQueryHandler = new SQLQueryHandler(database);

            MuServer server = MuServerBuilder.httpServer()
                    .addHandler(RestHandlerBuilder.restHandler(new JsonResource(jsonDatabaseHandler, sqlQueryHandler))
                            .addCustomWriter(jacksonJsonProvider)
                            .addCustomReader(jacksonJsonProvider))
                    .start();

            System.out.println("服务器已启动，访问地址: " + server.uri().resolve("/json"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
