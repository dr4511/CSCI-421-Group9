package WhereTree;

import Catalog.TableSchema;
import Common.Record;

/**
 * Interface for where clause tree node
 */
public interface IWhereTree {

    boolean evaluate(Record record, TableSchema schema);
}
