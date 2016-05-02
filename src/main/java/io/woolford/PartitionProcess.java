package io.woolford;


import org.apache.hadoop.record.Record;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PartitionProcess extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {

    private static final long serialVersionUID = -1919222653470217466L;

    static Logger logger = Logger.getLogger(PartitionProcess.class.getName());

    @Override
    public BoxedUnit apply(Iterator<Row> iterator) {

        List<Record> records = new ArrayList<>();
        while (iterator.hasNext()) {
            logger.info(iterator.next());
        }

        return BoxedUnit.UNIT;
    }
}
