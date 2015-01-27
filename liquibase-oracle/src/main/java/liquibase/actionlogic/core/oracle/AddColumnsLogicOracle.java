package liquibase.actionlogic.core.oracle;

import liquibase.Scope;
import liquibase.action.Action;
import liquibase.action.core.ColumnDefinition;
import liquibase.actionlogic.core.AddColumnsLogic;
import liquibase.database.Database;
import liquibase.database.core.oracle.OracleDatabase;
import liquibase.datatype.DataTypeFactory;

public class AddColumnsLogicOracle extends AddColumnsLogic {

    @Override
    protected int getPriority() {
        return PRIORITY_SPECIALIZED;
    }

    @Override
    protected boolean supportsScope(Scope scope) {
        return super.supportsScope(scope) && scope.get(Scope.Attr.database, Database.class) instanceof OracleDatabase;
    }

    @Override
    protected String getDefaultValueClause(ColumnDefinition column, Action action, Scope scope) {
        Database database = scope.get(Scope.Attr.database, Database.class);
        Object defaultValue = column.get(ColumnDefinition.Attr.defaultValue, Object.class);
        if (defaultValue != null) {
            if (defaultValue.toString().startsWith("GENERATED ALWAYS ")) {
                return DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database);
            } else {
               return super.getDefaultValueClause(column, action, scope);
            }
        }
        return null;

    }
}
