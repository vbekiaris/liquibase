package liquibase.parser;

import liquibase.database.Database;
import liquibase.exception.LiquibaseParseException;
import liquibase.resource.ResourceAccessor;
import liquibase.snapshot.DatabaseSnapshot;

public interface SnapshotParser extends LiquibaseParser {

    public DatabaseSnapshot parse(String path, Database database, ResourceAccessor resourceAccessor) throws LiquibaseParseException;

    boolean supports(String path, ResourceAccessor resourceAccessor);

}