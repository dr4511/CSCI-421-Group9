package WhereTree;

import Catalog.TableSchema;
import Common.Record;

public interface IOperandNode {
    Object getValue(Record record, TableSchema table);
}