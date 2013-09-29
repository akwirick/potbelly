package com.akwirick.potbelly;

import com.google.common.net.InetAddresses;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Pig UDF that converts a String field containing an IP Address into a Long
 */
public class IPToInt extends EvalFunc<Long> {

  @Override
  public Long exec(Tuple input) throws IOException {
    if (input == null || input.size() < 1 || input.get(0) == null) {
      return null;
    }
    return InetAddresses.forString((String) input.get(0)).hashCode() & 0xFFFFFFFFL;
  }

  /**Let's Pig know that it is getting a Long back
   *
   * @return  The schema returned by the UDF
   */
  @Override
  public Schema outputSchema(Schema input) {
    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
  }

  /**Checks the schema on the incoming Tuple to make sure we only get CHARARRAY data.
   *
   * @return The arguments passed to the UDF
   * @throws FrontendException
   */
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
    return funcList;
  }
}