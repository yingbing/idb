package idb.core;

import idb.model.Record;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SQLQueryHandler {
    private Database database;

    public SQLQueryHandler(Database database) {
        this.database = database;
    }

    public Set<Record> executeQuery(String sql) throws JSQLParserException, ReflectiveOperationException, IOException {
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Select) {
            PlainSelect plainSelect = (PlainSelect) ((Select) statement).getSelectBody();
            String tableName = plainSelect.getFromItem().toString();
            Table table = database.getTable(tableName);

            Map<String, Object> conditions = new HashMap<>();
            Expression where = plainSelect.getWhere();
            if (where != null) {
                parseExpression(where, conditions);
            }

            return table.query(conditions);
        }
        throw new UnsupportedOperationException("Only SELECT queries are supported");
    }

    private void parseExpression(Expression expression, Map<String, Object> conditions) {
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            String columnName = equalsTo.getLeftExpression().toString();
            String value = equalsTo.getRightExpression().toString().replaceAll("'", "");
            conditions.put(columnName, value);
        }
        // 添加更多的表达式解析逻辑，以支持更复杂的查询条件
    }
}
