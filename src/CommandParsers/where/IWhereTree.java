package CommandParsers.where;

import Common.Record;

public interface IWhereTree {
    boolean evaluate(Record record);
}